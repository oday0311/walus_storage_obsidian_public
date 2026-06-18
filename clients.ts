import type { Signer } from "@mysten/sui/cryptography";
import { decodeSuiPrivateKey } from "@mysten/sui/cryptography";
import { SuiClient, getFullnodeUrl, type SuiParsedData } from "@mysten/sui/client";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { Secp256k1Keypair } from "@mysten/sui/keypairs/secp256k1";
import { Secp256r1Keypair } from "@mysten/sui/keypairs/secp256r1";
import { WalrusClient } from "@mysten/walrus";

export type NetworkType = "testnet" | "mainnet";

export interface ClientConfig {
	suiNetwork: NetworkType;
	walrusNetwork: NetworkType;
}

export interface UploadBlobParams {
	blob: Uint8Array;
	epochs: number;
	signer: Signer;
	owner?: string;
	attributes?: Record<string, string>;
}

export interface OwnedBlobRecord {
	blobId: string;
	blobObjectId: string;
	size: number | null;
	type: string | null;
}

export interface TokenBalanceSummary {
	sui: {
		coinType: string;
		symbol: string;
		totalBalance: string;
		decimals: number;
		formatted: string;
	};
	wal: {
		coinType: string;
		symbol: string;
		totalBalance: string;
		decimals: number;
		formatted: string;
	};
}

export class WalrusStorageClients {
	private suiClient: SuiClient | null = null;
	private walrusClient: WalrusClient | null = null;
	private config: ClientConfig;
	private static TESTNET_WAL_COIN_TYPE =
		"0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL";
	private static MAINNET_WAL_COIN_TYPE =
		"0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL";
	private static TESTNET_AGGREGATORS = [
		"https://aggregator.walrus-testnet.walrus.space",
		"https://aggregator.walrus-testnet.h2o-nodes.com",
		"https://aggregator.testnet.walrus.mirai.cloud",
		"https://testnet-aggregator.walrus.graphyte.dev",
		"https://walrus-testnet-aggregator.chainflow.io",
	];
	private static MAINNET_AGGREGATORS = [
		"https://aggregator.walrus-mainnet.walrus.space",
		"https://aggregator.walrus-mainnet.h2o-nodes.com",
		"https://aggregator.mainnet.walrus.mirai.cloud",
		"https://mainnet-aggregator.walrus.graphyte.dev",
		"https://walmain.agg.chainflow.io",
	];

	constructor(config: ClientConfig) {
		this.config = config;
	}

	getSuiClient(): SuiClient {
		if (!this.suiClient) {
			const rpcUrl = getFullnodeUrl(this.config.suiNetwork);
			this.suiClient = new SuiClient({ url: rpcUrl });
		}
		return this.suiClient;
	}

	getWalrusClient(): WalrusClient {
		if (!this.walrusClient) {
			this.walrusClient = new WalrusClient({
				network: this.config.walrusNetwork,
				suiRpcUrl: getFullnodeUrl(this.config.suiNetwork),
				wasmUrl: "https://unpkg.com/@mysten/walrus-wasm@0.2.0/web/walrus_wasm_bg.wasm",
			});
		}
		return this.walrusClient;
	}

	updateNetwork(config: ClientConfig): void {
		this.config = config;
		this.suiClient = null;
		this.walrusClient = null;
	}

	createSigner(privateKey: string): Signer {
		const trimmedKey = privateKey.trim();
		const parsed = decodeSuiPrivateKey(trimmedKey);

		switch (parsed.scheme) {
			case "ED25519":
				return Ed25519Keypair.fromSecretKey(trimmedKey);
			case "Secp256k1":
				return Secp256k1Keypair.fromSecretKey(trimmedKey);
			case "Secp256r1":
				return Secp256r1Keypair.fromSecretKey(trimmedKey);
			default:
				throw new Error(`Unsupported Sui private key scheme: ${parsed.scheme}`);
		}
	}

	getAddressFromPrivateKey(privateKey: string): string {
		return this.createSigner(privateKey).toSuiAddress();
	}

	deriveEd25519KeypairFromMnemonic(mnemonics: string, path?: string): Ed25519Keypair {
		const normalized = mnemonics.trim().replace(/\s+/g, " ");
		if (!normalized) {
			throw new Error("Mnemonic cannot be empty.");
		}

		return Ed25519Keypair.deriveKeypair(normalized, path?.trim() || undefined);
	}

	async uploadBlob({
		blob,
		epochs,
		signer,
		owner,
		attributes,
	}: UploadBlobParams): Promise<{ blobId: string; blobObjectId: string; size: number }> {
		const walrusClient = this.getWalrusClient();
		const traceId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;

		// #region debug-point B:writeBlob-start
		(() => {
			const u = "http://127.0.0.1:7777/event";
			const s = "upload-fails-balances";
			fetch(u, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					sessionId: s,
					runId: "post-fix",
					hypothesisId: "B",
					location: "clients.ts:uploadBlob",
					traceId,
					msg: "[DEBUG] writeBlob start",
					data: {
						epochs,
						blobSize: blob.byteLength,
						owner,
						signerAddress: signer.toSuiAddress(),
						hasAttributes: Boolean(attributes),
						attributeKeys: attributes ? Object.keys(attributes) : [],
					},
					ts: Date.now(),
				}),
			}).catch(() => {});
		})();
		// #endregion

		const result = await walrusClient
			.writeBlob({
				blob,
				deletable: true,
				epochs,
				signer,
				owner,
				attributes,
			})
			.catch((error: unknown) => {
				// #region debug-point C:writeBlob-error
				(() => {
					const u = "http://127.0.0.1:7777/event";
					const s = "upload-fails-balances";
					fetch(u, {
						method: "POST",
						headers: { "Content-Type": "application/json" },
						body: JSON.stringify({
							sessionId: s,
							runId: "post-fix",
							hypothesisId: "C",
							location: "clients.ts:uploadBlob:writeBlob",
							traceId,
							msg: "[DEBUG] writeBlob error",
							data: {
								error:
									error instanceof Error
										? {
												name: error.name,
												message: error.message,
												stack: error.stack,
											}
										: { value: String(error) },
							},
							ts: Date.now(),
						}),
					}).catch(() => {});
				})();
				// #endregion
				throw error;
			});

		// #region debug-point B:writeBlob-success
		(() => {
			const u = "http://127.0.0.1:7777/event";
			const s = "upload-fails-balances";
			fetch(u, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					sessionId: s,
					runId: "post-fix",
					hypothesisId: "B",
					location: "clients.ts:uploadBlob",
					traceId,
					msg: "[DEBUG] writeBlob success",
					data: {
						blobId: result.blobId,
						blobObjectId: result.blobObject.id.id,
						size: result.blobObject.size,
					},
					ts: Date.now(),
				}),
			}).catch(() => {});
		})();
		// #endregion

		return {
			blobId: result.blobId,
			blobObjectId: result.blobObject.id.id,
			size: Number(result.blobObject.size),
		};
	}

	async downloadBlob(blobId: string): Promise<Uint8Array> {
		const walrusClient = this.getWalrusClient();
		const traceId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;

		// #region debug-point D:download-start
		(() => {
			fetch("http://127.0.0.1:7778/event", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					sessionId: "download-blob-metadata",
					runId: "pre-fix",
					hypothesisId: "D",
					location: "clients.ts:downloadBlob:start",
					traceId,
					msg: "[DEBUG] downloadBlob start",
					data: { blobId },
					ts: Date.now(),
				}),
			}).catch(() => {});
		})();
		// #endregion

		try {
			const metadata = await walrusClient.getBlobMetadata({ blobId });

			// #region debug-point D:metadata-success
			(() => {
				fetch("http://127.0.0.1:7778/event", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						sessionId: "download-blob-metadata",
						runId: "pre-fix",
						hypothesisId: "D",
						location: "clients.ts:downloadBlob:metadata",
						traceId,
						msg: "[DEBUG] getBlobMetadata success",
						data: {
							blobId,
							metadataKind: metadata.metadata.$kind,
							metadataBlobId: metadata.blobId,
						},
						ts: Date.now(),
					}),
				}).catch(() => {});
			})();
			// #endregion
		} catch (error) {
			// #region debug-point D:metadata-error
			(() => {
				fetch("http://127.0.0.1:7778/event", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						sessionId: "download-blob-metadata",
						runId: "pre-fix",
						hypothesisId: "D",
						location: "clients.ts:downloadBlob:metadata",
						traceId,
						msg: "[DEBUG] getBlobMetadata error",
						data: {
							blobId,
							error:
								error instanceof Error
									? { name: error.name, message: error.message, stack: error.stack }
									: { value: String(error) },
						},
						ts: Date.now(),
					}),
				}).catch(() => {});
			})();
			// #endregion
		}

		return walrusClient.readBlob({ blobId }).catch((error: unknown) => {
			// #region debug-point D:download-error
			(() => {
				fetch("http://127.0.0.1:7778/event", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						sessionId: "download-blob-metadata",
						runId: "pre-fix",
						hypothesisId: "D",
						location: "clients.ts:downloadBlob:readBlob",
						traceId,
						msg: "[DEBUG] readBlob error",
						data: {
							blobId,
							error:
								error instanceof Error
									? { name: error.name, message: error.message, stack: error.stack }
									: { value: String(error) },
						},
						ts: Date.now(),
					}),
				}).catch(() => {});
			})();
			// #endregion
			throw error;
		});
	}

	async downloadBlobByObjectId(blobObjectId: string): Promise<Uint8Array> {
		return this.downloadFromAggregators(
			`/v1/blobs/by-object-id/${encodeURIComponent(blobObjectId)}`,
			"Object ID download failed",
		);
	}

	async getAddressBalances(owner: string): Promise<TokenBalanceSummary> {
		const suiClient = this.getSuiClient();
		const walCoinType =
			this.config.walrusNetwork === "testnet"
				? WalrusStorageClients.TESTNET_WAL_COIN_TYPE
				: WalrusStorageClients.MAINNET_WAL_COIN_TYPE;

		const [suiBalance, walBalance, suiMetadata, walMetadata] = await Promise.all([
			suiClient.getBalance({ owner, coinType: "0x2::sui::SUI" }),
			suiClient.getBalance({ owner, coinType: walCoinType }),
			suiClient.getCoinMetadata({ coinType: "0x2::sui::SUI" }),
			suiClient.getCoinMetadata({ coinType: walCoinType }),
		]);

		return {
			sui: this.formatBalanceSummary(
				"0x2::sui::SUI",
				suiBalance.totalBalance,
				suiMetadata?.decimals ?? 9,
				suiMetadata?.symbol ?? "SUI",
			),
			wal: this.formatBalanceSummary(
				walCoinType,
				walBalance.totalBalance,
				walMetadata?.decimals ?? 9,
				walMetadata?.symbol ?? "WAL",
			),
		};
	}

	async listOwnedBlobs(owner: string): Promise<OwnedBlobRecord[]> {
		const suiClient = this.getSuiClient();
		const walrusClient = this.getWalrusClient();
		const blobType = await walrusClient.getBlobType();
		const response = await suiClient.getOwnedObjects({
			owner,
			filter: { StructType: blobType },
			options: {
				showType: true,
				showContent: true,
			},
		});

		return response.data.map((item) => {
			const content = item.data?.content as SuiParsedData | undefined;
			const fields =
				content && content.dataType === "moveObject"
					? (content.fields as Record<string, unknown>)
					: null;
			const rawBlobId = fields?.blob_id;
			const rawSize = fields?.size;
			const normalizedBlobId = this.normalizeBlobId(rawBlobId, item.data?.objectId ?? "");

			// #region debug-point A:list-owned-blob
			(() => {
				fetch("http://127.0.0.1:7778/event", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						sessionId: "download-blob-metadata",
						runId: "pre-fix",
						hypothesisId: "A",
						location: "clients.ts:listOwnedBlobs",
						msg: "[DEBUG] owned blob mapped",
						data: {
							objectId: item.data?.objectId ?? "",
							rawBlobId,
							rawBlobIdType: Array.isArray(rawBlobId) ? "array" : typeof rawBlobId,
							normalizedBlobId,
							type: item.data?.type ?? null,
						},
						ts: Date.now(),
					}),
				}).catch(() => {});
			})();
			// #endregion

			return {
				blobId: normalizedBlobId,
				blobObjectId: item.data?.objectId ?? "",
				size:
					typeof rawSize === "string"
						? Number(rawSize)
						: typeof rawSize === "number"
							? rawSize
							: null,
				type: item.data?.type ?? null,
			};
		});
	}

	async testConnection(): Promise<{ sui: boolean; walrus: boolean }> {
		const results = { sui: false, walrus: false };

		try {
			const suiClient = this.getSuiClient();
			await suiClient.getLatestCheckpointSequenceNumber();
			results.sui = true;
		} catch (error) {
			console.error("Sui connection test failed:", error);
		}

		try {
			const walrusClient = this.getWalrusClient();
			await walrusClient.systemState();
			results.walrus = true;
		} catch (error) {
			console.error("Walrus connection test failed:", error);
		}

		return results;
	}

	private normalizeBlobId(rawBlobId: unknown, fallback: string): string {
		try {
			const bcsUtils = require("@mysten/walrus/dist/cjs/utils/bcs.js") as {
				blobIdFromBytes: (blobId: Uint8Array) => string;
				blobIdFromInt: (blobId: bigint | string) => string;
			};

			if (typeof rawBlobId === "string") {
				return /^\d+$/.test(rawBlobId)
					? bcsUtils.blobIdFromInt(rawBlobId)
					: rawBlobId;
			}

			if (typeof rawBlobId === "number" || typeof rawBlobId === "bigint") {
				return bcsUtils.blobIdFromInt(String(rawBlobId));
			}

			if (rawBlobId instanceof Uint8Array) {
				return bcsUtils.blobIdFromBytes(rawBlobId);
			}

			if (Array.isArray(rawBlobId) && rawBlobId.every((value) => typeof value === "number")) {
				return bcsUtils.blobIdFromBytes(new Uint8Array(rawBlobId));
			}

			if (
				rawBlobId &&
				typeof rawBlobId === "object" &&
				"bytes" in rawBlobId &&
				Array.isArray((rawBlobId as { bytes?: unknown }).bytes)
			) {
				return bcsUtils.blobIdFromBytes(
					new Uint8Array((rawBlobId as { bytes: number[] }).bytes),
				);
			}
		} catch (error) {
			// #region debug-point B:normalize-error
			(() => {
				fetch("http://127.0.0.1:7778/event", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						sessionId: "download-blob-metadata",
						runId: "pre-fix",
						hypothesisId: "B",
						location: "clients.ts:normalizeBlobId",
						msg: "[DEBUG] normalizeBlobId error",
						data: {
							rawBlobId,
							fallback,
							error:
								error instanceof Error
									? { name: error.name, message: error.message, stack: error.stack }
									: { value: String(error) },
						},
						ts: Date.now(),
					}),
				}).catch(() => {});
			})();
			// #endregion
			console.error("Failed to normalize blob ID:", rawBlobId, error);
		}

		return fallback;
	}

	private async downloadFromAggregators(
		pathname: string,
		errorPrefix: string,
	): Promise<Uint8Array> {
		const errors: string[] = [];

		for (const baseUrl of this.getAggregatorUrls()) {
			try {
				const response = await fetch(`${baseUrl}${pathname}`);
				if (!response.ok) {
					errors.push(`${baseUrl} -> ${response.status}`);
					continue;
				}

				return new Uint8Array(await response.arrayBuffer());
			} catch (error) {
				errors.push(
					`${baseUrl} -> ${
						error instanceof Error ? error.message : String(error)
					}`,
				);
			}
		}

		throw new Error(`${errorPrefix}. Aggregators tried: ${errors.join("; ")}`);
	}

	private getAggregatorUrls(): string[] {
		return this.config.walrusNetwork === "mainnet"
			? WalrusStorageClients.MAINNET_AGGREGATORS
			: WalrusStorageClients.TESTNET_AGGREGATORS;
	}

	private formatBalanceSummary(
		coinType: string,
		totalBalance: string,
		decimals: number,
		symbol: string,
	): TokenBalanceSummary["sui"] {
		return {
			coinType,
			symbol,
			totalBalance,
			decimals,
			formatted: this.formatAmount(totalBalance, decimals),
		};
	}

	private formatAmount(rawAmount: string, decimals: number): string {
		const normalized = rawAmount.replace(/^0+(\d)/, "$1");
		const digits = normalized === "" ? "0" : normalized;
		if (decimals <= 0) {
			return digits;
		}

		if (digits.length <= decimals) {
			const paddedFraction = digits.padStart(decimals, "0").replace(/0+$/, "");
			return paddedFraction ? `0.${paddedFraction}` : "0";
		}

		const integerPart = digits.slice(0, digits.length - decimals);
		const fractionPart = digits.slice(digits.length - decimals).replace(/0+$/, "");
		return fractionPart ? `${integerPart}.${fractionPart}` : integerPart;
	}
}
