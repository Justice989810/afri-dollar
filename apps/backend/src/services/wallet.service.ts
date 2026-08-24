import { Keypair } from '@stellar/stellar-sdk';
import { createClient } from 'redis';

import prisma from '../config/database';
import { AppError } from '../types';
import type {
  CreateWalletOptions,
  DeleteWalletRequest,
  ListWalletsOptions,
  PaginatedWallets,
  Wallet,
  WalletBalance,
  WalletTransaction,
  WalletType,
  WalletNetwork,
  WalletWithKeys,
} from '../types';
import { encrypt } from '../utils/crypto';

import { AuditService } from './audit.service';
import { AuthService } from './auth.service';
import { StellarService } from './stellar.service';
import { WebhookService } from './webhook.service';

type RedisClient = ReturnType<typeof createClient>;
let redisClient: RedisClient | null = null;
let redisConnectPromise: Promise<RedisClient | null> | null = null;
const memoryCache = new Map<string, { value: string; expiresAt: number }>();

async function getRedisClient(): Promise<RedisClient | null> {
  if (!process.env.REDIS_URL) {
    return null;
  }

  if (redisClient && redisClient.isOpen) {
    return redisClient;
  }

  if (!redisConnectPromise) {
    redisConnectPromise = (async (): Promise<RedisClient | null> => {
      try {
        const client = createClient({ url: process.env.REDIS_URL });
        client.on('error', (err) => {
          console.error('Redis wallet cache error:', err);
          redisClient = null;
          redisConnectPromise = null;
        });
        await client.connect();
        redisClient = client;
        return client;
      } catch (error) {
        console.error('Redis wallet cache unavailable, using memory fallback:', error);
        redisClient = null;
        redisConnectPromise = null;
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
   * Returns all active wallets for the authenticated user, supporting filtering & pagination.
   */
  async listWallets(options: ListWalletsOptions): Promise<PaginatedWallets> {
    const page = options.page && options.page > 0 ? options.page : 1;
    const limit = options.limit && options.limit > 0 ? Math.min(options.limit, 100) : 20;
    const skip = (page - 1) * limit;

    const where: {
      userId: string;
      isActive: boolean;
      walletType?: string;
      network?: string;
    } = {
      userId: options.userId,
      isActive: true,
    };

    if (options.walletType) {
      where.walletType = options.walletType;
    }

    if (options.network) {
      where.network = options.network;
    }

    const [wallets, total] = await Promise.all([
      prisma.wallet.findMany({
        where,
        skip,
        take: limit,
        orderBy: { createdAt: 'desc' },
      }),
      prisma.wallet.count({ where }),
    ]);

    const data: Wallet[] = wallets.map((w) => ({
      id: w.id,
      userId: w.userId,
      walletType: w.walletType as WalletType,
      network: w.network as WalletNetwork,
      publicKey: w.publicKey,
      status: w.isActive ? 'active' : 'archived',
      createdAt: w.createdAt,
      updatedAt: w.updatedAt,
    }));

    return {
      data,
      pagination: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit) || 1,
      },
    };
  },

  /**
   * Returns a single wallet by ID ensuring ownership.
   * Includes metadata and cached/last-known XLM balance.
   */
  async getWalletById(walletId: string, userId: string): Promise<Wallet> {
    const wallet = await prisma.wallet.findUnique({
      where: { id: walletId },
      include: { balances: true },
    });

    if (!wallet || wallet.userId !== userId || !wallet.isActive) {
      throw new AppError(404, 'Wallet not found');
    }

    // Check cached balance
    let lastKnownBalance: string | undefined;
    const cachedBalances = await getFromCache<WalletBalance[]>(getBalanceCacheKey(walletId));
    if (cachedBalances && cachedBalances.length > 0) {
      const nativeBalance = cachedBalances.find((b) => b.asset_type === 'native');
      if (nativeBalance) {
        lastKnownBalance = nativeBalance.balance;
      }
    }

    if (!lastKnownBalance && wallet.balances && wallet.balances.length > 0) {
      const dbNative = wallet.balances.find(
        (b) => b.assetCode === 'XLM' || b.assetCode === 'native'
      );
      if (dbNative) {
        lastKnownBalance = dbNative.balance;
      }
    }

    await AuditService.log({
      action: 'wallet.viewed',
      resource: 'wallet',
      resourceId: wallet.id,
      userId,
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
      lastKnownBalance,
    };
  },

  /**
   * Fetches real-time balances for the wallet from Stellar Horizon.
   * Caches balance for 30 seconds in Redis. Gracefully handles un-funded accounts.
   */
  async getWalletBalances(walletId: string, userId: string): Promise<WalletBalance[]> {
    const wallet = await prisma.wallet.findUnique({
      where: { id: walletId },
    });

    if (!wallet || wallet.userId !== userId || !wallet.isActive) {
      throw new AppError(404, 'Wallet not found');
    }

    const cacheKey = getBalanceCacheKey(walletId);
    const cached = await getFromCache<WalletBalance[]>(cacheKey);
    if (cached) {
      await AuditService.log({
        action: 'wallet.balances.viewed',
        resource: 'wallet',
        resourceId: wallet.id,
        userId,
        metadata: { cached: true },
      });
      return cached;
    }

    let balances: WalletBalance[] = [];

    try {
      const horizonServer = StellarService.getHorizonServer();
      const account = await horizonServer.loadAccount(wallet.publicKey);

      balances = account.balances
        .filter(
          (b) =>
            b.asset_type === 'native' ||
            b.asset_type === 'credit_alphanum4' ||
            b.asset_type === 'credit_alphanum12'
        )
        .map((b) => {
          const item: WalletBalance = {
            asset_type: b.asset_type,
            balance: b.balance,
          };
          if ('asset_code' in b && b.asset_code) item.asset_code = b.asset_code;
          if ('asset_issuer' in b && b.asset_issuer) item.asset_issuer = b.asset_issuer;
          if ('buying_liabilities' in b && b.buying_liabilities)
            item.buying_liabilities = b.buying_liabilities;
          if ('selling_liabilities' in b && b.selling_liabilities)
            item.selling_liabilities = b.selling_liabilities;
          if ('limit' in b && b.limit) item.limit = b.limit;
          return item;
        });
    } catch (error: unknown) {
      const err = error as Record<string, unknown>;
      const isNotFound =
        (err &&
          typeof err.response === 'object' &&
          (err.response as Record<string, unknown>)?.status === 404) ||
        (error instanceof AppError && error.status === 404);

      if (isNotFound) {
        balances = [];
      } else {
        throw new AppError(
          502,
          `Failed to fetch account balances from Stellar: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
      }
    }

    // Cache balance for 30 seconds
    await setToCache(cacheKey, balances, 30);

    await AuditService.log({
      action: 'wallet.balances.viewed',
      resource: 'wallet',
      resourceId: wallet.id,
      userId,
      metadata: { cached: false, count: balances.length },
    });

    return balances;
  },

  /**
   * Fetches paginated transaction history for the wallet from Stellar Horizon.
   * Gracefully handles un-funded accounts.
   */
  async getWalletTransactions(
    walletId: string,
    userId: string,
    options?: { limit?: number; cursor?: string }
  ): Promise<WalletTransaction[]> {
    const wallet = await prisma.wallet.findUnique({
      where: { id: walletId },
    });

    if (!wallet || wallet.userId !== userId || !wallet.isActive) {
      throw new AppError(404, 'Wallet not found');
    }

    try {
      const rawRecords = await StellarService.getAccountTransactions(wallet.publicKey, options);

      return rawRecords.map((tx) => ({
        id: tx.id,
        createdAt: new Date(tx.created_at),
        type: 'payment',
        successful: tx.successful,
        feeCharged: tx.fee_charged !== undefined ? String(tx.fee_charged) : undefined,
        memo: tx.memo,
        memoType: tx.memo_type,
        paging_token: tx.paging_token,
      }));
    } catch (error: unknown) {
      const err = error as Record<string, unknown>;
      const isNotFound =
        (err &&
          typeof err.response === 'object' &&
          (err.response as Record<string, unknown>)?.status === 404) ||
        (error instanceof AppError &&
          error.status === 404 &&
          (error.message.includes('not found') || error.message.includes('Stellar account')));

      if (isNotFound) {
        return [];
      }

      if (error instanceof AppError) throw error;
      throw new AppError(
        502,
        `Failed to fetch account transactions: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  },

  /**
   * Soft-deletes a wallet (sets status = 'archived' / isActive = false in DB).
   * Prevents deletion if wallet has non-zero balance on Stellar Horizon.
   * Requires password confirmation or 2FA token.
   */
  async deleteWallet(
    walletId: string,
    userId: string,
    confirmation: DeleteWalletRequest
  ): Promise<void> {
    const user = await prisma.user.findUnique({
      where: { id: userId },
    });

    if (!user) {
      throw new AppError(404, 'User not found');
    }

    if (confirmation.password) {
      if (!user.passwordHash) {
        throw new AppError(400, 'User password is not set');
      }
      const isMatch = await AuthService.verifyPassword(confirmation.password, user.passwordHash);
      if (!isMatch) {
        throw new AppError(400, 'Invalid password confirmation');
      }
    } else if (confirmation.twoFactorToken) {
      // Placeholder for future 2FA token verification
      if (confirmation.twoFactorToken.length < 6) {
        throw new AppError(400, 'Invalid 2FA token');
      }
    } else {
      throw new AppError(400, 'Password or 2FA token confirmation is required');
    }

    const wallet = await prisma.wallet.findUnique({
      where: { id: walletId },
    });

    if (!wallet || wallet.userId !== userId || !wallet.isActive) {
      throw new AppError(404, 'Wallet not found');
    }

    // Check real-time balance on Stellar Horizon (not cached value)
    try {
      const horizonServer = StellarService.getHorizonServer();
      const account = await horizonServer.loadAccount(wallet.publicKey);

      const hasBalance = account.balances.some((b) => {
        const val = parseFloat(b.balance);
        return !isNaN(val) && val > 0;
      });

      if (hasBalance) {
        throw new AppError(
          400,
          'WALLET_HAS_BALANCE: Wallet has a non-zero balance. Please transfer all funds before deleting.'
        );
      }
    } catch (error: unknown) {
      if (error instanceof AppError) {
        throw error;
      }

      const err = error as Record<string, unknown>;
      const isNotFound =
        err &&
        typeof err.response === 'object' &&
        (err.response as Record<string, unknown>)?.status === 404;

      if (!isNotFound) {
        throw new AppError(
          502,
          `Failed to verify wallet balance on Stellar: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
      }
      // Account not found on Horizon means 0 balance / unfunded account, safe to delete.
    }

    // Soft delete in database
    await prisma.wallet.update({
      where: { id: walletId },
      data: { isActive: false },
    });

    // Invalidate Redis balance cache
    await WalletService.invalidateBalanceCache(walletId);

    // Emit event & log audit
    await WebhookService.emitEvent({
      eventType: 'wallet.archived',
      payload: { walletId: wallet.id, publicKey: wallet.publicKey },
      userId,
    });

    await AuditService.log({
      action: 'wallet.archived',
      resource: 'wallet',
      resourceId: wallet.id,
      userId,
      metadata: { publicKey: wallet.publicKey, walletType: wallet.walletType },
    });
  },
};
