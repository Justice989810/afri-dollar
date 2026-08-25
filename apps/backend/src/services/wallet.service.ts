import { Keypair } from '@stellar/stellar-sdk';
import { createClient } from 'redis';

import prisma from '../config/database';
import { AppError } from '../types';
import type { CreateWalletOptions, WalletWithKeys } from '../types';
import type { PaymentRecord } from '../types/transaction.types';
import { encrypt } from '../utils/crypto';

import { AuditService } from './audit.service';
import { AuthService } from './auth.service';
import { StellarService } from './stellar.service';
import { TransactionService } from './transaction.service';
import { WebhookService } from './webhook.service';

function parseStellarStroops(amount: string): bigint {
  const [whole = '0', frac = ''] = amount.split('.');
  const paddedFrac = frac.padEnd(7, '0').slice(0, 7);
  return BigInt(whole || '0') * 10_000_000n + BigInt(paddedFrac);
}

type RedisClient = ReturnType<typeof createClient>;
let redisClient: RedisClient | null = null;
let redisConnectPromise: Promise<RedisClient | null> | null = null;
let redisConnectionGeneration = 0;
const memoryCache = new Map<string, { value: string; expiresAt: number }>();
const MEMORY_CACHE_MAX_ENTRIES = 5000;

function pruneMemoryCache(): void {
  const now = Date.now();
  for (const [key, item] of memoryCache) {
    if (now > item.expiresAt) memoryCache.delete(key);
  }

  while (memoryCache.size >= MEMORY_CACHE_MAX_ENTRIES) {
    const oldest = memoryCache.keys().next();
    if (oldest.done) break;
    memoryCache.delete(oldest.value);
  }
}

async function getRedisClient(): Promise<RedisClient | null> {
  if (!process.env.REDIS_URL) {
    return null;
  }

  if (redisClient && redisClient.isOpen) {
    return redisClient;
  }

  if (!redisConnectPromise) {
    const connectionGeneration = ++redisConnectionGeneration;
    redisConnectPromise = (async (): Promise<RedisClient | null> => {
      let client: RedisClient | null = null;
      try {
        client = createClient({ url: process.env.REDIS_URL });
        client.on('error', (err) => {
          console.error('Redis wallet cache error:', err);
          if (redisConnectionGeneration === connectionGeneration) {
            redisConnectionGeneration++;
            if (redisClient === client) redisClient = null;
            redisConnectPromise = null;
          }
          client?.disconnect().catch(() => {});
        });
        await client.connect();
        if (redisConnectionGeneration !== connectionGeneration) {
          client.disconnect().catch(() => {});
          return null;
        }
        redisClient = client;
        return client;
      } catch (error) {
        console.error('Redis wallet cache unavailable, using memory fallback:', error);
        if (client) {
          client.disconnect().catch(() => {});
        }
        if (redisConnectionGeneration === connectionGeneration) {
          redisConnectionGeneration++;
          if (redisClient === client) redisClient = null;
          redisConnectPromise = null;
        }
        return null;
      }
    })();
  }

  return redisConnectPromise;
}

async function getFromCache<T>(key: string): Promise<T | null> {
  try {
    const redis = await getRedisClient();
    if (redis) {
      const raw = await redis.get(key);
      if (!raw) return null;
      return JSON.parse(raw) as T;
    }

    const item = memoryCache.get(key);
    if (!item) return null;
    if (Date.now() > item.expiresAt) {
      memoryCache.delete(key);
      return null;
    }
    return JSON.parse(item.value) as T;
  } catch {
    return null;
  }
}

async function setToCache(key: string, value: unknown, ttlSeconds: number): Promise<void> {
  try {
    const serialized = JSON.stringify(value);
    const redis = await getRedisClient();
    if (redis) {
      await redis.set(key, serialized, { EX: ttlSeconds });
      return;
    }

    pruneMemoryCache();
    memoryCache.set(key, {
      value: serialized,
      expiresAt: Date.now() + ttlSeconds * 1000,
    });
  } catch (err) {
    console.error('Failed to set wallet cache:', err);
  }
}

async function deleteFromCache(key: string): Promise<void> {
  try {
    const redis = await getRedisClient();
    if (redis) {
      await redis.del(key);
      return;
    }

    memoryCache.delete(key);
  } catch (err) {
    console.error('Failed to delete from wallet cache:', err);
  }
}

function getBalanceCacheKey(walletId: string): string {
  return `wallet:balance:${walletId}`;
}

export const WalletService = {
  /**
   * Invalidate cached balance for a wallet
   */
  async invalidateBalanceCache(walletId: string): Promise<void> {
    await deleteFromCache(getBalanceCacheKey(walletId));
  },

  /**
   * Clears in-memory cache (for testing purposes)
   */
  clearCacheForTesting(): void {
    memoryCache.clear();
  },

  /**
   * Creates a new Stellar wallet, encrypts the secret key, funds via Friendbot if testnet,
   * stores it in the database, emits webhook, and creates audit log.
   */
  async createWallet(options: CreateWalletOptions): Promise<WalletWithKeys> {
    const user = await prisma.user.findUnique({
      where: { id: options.userId },
    });

    if (!user) {
      throw new AppError(404, 'User not found');
    }

    const keypair = Keypair.random();
    const publicKey = keypair.publicKey();
    const secretKey = keypair.secret();

    const secretKeyEncrypted = encrypt(secretKey);

    if (options.network === 'testnet') {
      await StellarService.fundTestnetAccount(publicKey);
    }

    const wallet = await prisma.wallet.create({
      data: {
        userId: options.userId,
        publicKey,
        secretKeyEncrypted,
        walletType: options.walletType,
        network: options.network,
        isActive: true,
      },
    });

    await WebhookService.emitEvent({
      eventType: 'wallet.created',
      payload: { walletId: wallet.id, walletType: wallet.walletType, network: wallet.network },
      userId: options.userId,
    });

    await AuditService.log({
      action: 'wallet.created',
      resource: 'wallet',
      resourceId: wallet.id,
      userId: options.userId,
      metadata: { walletType: wallet.walletType, network: wallet.network },
    });

    return {
      id: wallet.id,
      userId: wallet.userId,
      walletType: wallet.walletType as WalletType,
      network: wallet.network as WalletNetwork,
      publicKey: wallet.publicKey,
      status: wallet.isActive ? 'active' : 'archived',
      createdAt: wallet.createdAt,
      updatedAt: wallet.updatedAt,
      secretKey,
    };
  },

  /**
   * Sends funds directly from a wallet the user owns, moving real value on
   * Stellar via TransactionService (build → sign → submit → track).
   */
  async sendFromWallet(
    walletId: string,
    userId: string,
    params: {
      destination: string;
      amount: string;
      assetCode: string;
      assetIssuer?: string;
      memo?: string;
    }
  ): Promise<PaymentRecord> {
    const wallet = await prisma.wallet.findUnique({
      where: { id: walletId },
    });

    if (!wallet) {
      throw new AppError(404, 'Wallet not found');
    }
    if (wallet.userId !== userId) {
      throw new AppError(403, 'Wallet does not belong to user');
    }

    return TransactionService.buildAndSubmitPayment({
      sourceWalletId: wallet.id,
      userId,
      destination: params.destination,
      amount: params.amount,
      assetCode: params.assetCode,
      assetIssuer: params.assetIssuer,
      memo: params.memo,
    });
  },
};
