import {
	App,
	DropdownComponent,
	ItemView,
	Modal,
	Notice,
	Plugin,
	PluginSettingTab,
	Setting,
	TFile,
	TextAreaComponent,
	TextComponent,
	WorkspaceLeaf,
	normalizePath,
	setIcon,
} from "obsidian";
import type { Signer } from "@mysten/sui/cryptography";
import {
	type NetworkType,
	type OwnedBlobRecord,
	type TokenBalanceSummary,
	WalrusStorageClients,
} from "./clients";

interface WalrusUploadRecord {
	blobId: string;
	blobObjectId: string;
	sourcePath: string;
	sourceName: string;
	size: number;
	uploadedAt: string;
	suiAddress: string;
	suiNetwork: NetworkType;
	walrusNetwork: NetworkType;
}

interface WalrusStorageSettings {
	suiNetwork: NetworkType;
	walrusNetwork: NetworkType;
	suiAddress: string;
	walrusAddress: string;
	suiPrivateKey: string;
	walrusPrivateKey: string;
	storageEpochs: number;
	downloadFolder: string;
	uploads: WalrusUploadRecord[];
}

const DEFAULT_SETTINGS: WalrusStorageSettings = {
	suiNetwork: "testnet",
	walrusNetwork: "testnet",
	suiAddress: "",
	walrusAddress: "",
	suiPrivateKey: "",
	walrusPrivateKey: "",
	storageEpochs: 5,
	downloadFolder: "walrus-downloads",
	uploads: [],
};

const WALRUS_STORAGE_VIEW_TYPE = "walrus-storage-right-view";

export default class WalrusStoragePlugin extends Plugin {
	settings!: WalrusStorageSettings;
	clients!: WalrusStorageClients;
	settingTab!: WalrusStorageSettingTab;

	async onload(): Promise<void> {
		await this.loadSettings();
		this.initializeClients();
		this.settingTab = new WalrusStorageSettingTab(this.app, this);
		this.addSettingTab(this.settingTab);
		this.registerView(
			WALRUS_STORAGE_VIEW_TYPE,
			(leaf) => new WalrusStorageRightView(leaf, this),
		);
		this.addRibbonIcon("database", "Open Walrus Storage", () => {
			void this.openPluginHome();
		});

		this.addCommand({
			id: "show-current-config",
			name: "Show network config",
			callback: () => {
				new Notice(
					`Sui: ${this.settings.suiNetwork} | Walrus: ${this.settings.walrusNetwork}`,
				);
			},
		});

		this.addCommand({
			id: "derive-addresses-from-private-key",
			name: "Derive Sui/Walrus addresses from private key",
			callback: () => {
				void this.deriveAddressesFromPrivateKeys();
			},
		});

		this.addCommand({
			id: "prepare-note-for-upload",
			name: "Preview current note (before upload)",
			checkCallback: (checking: boolean) => {
				const file = this.app.workspace.getActiveFile();
				if (!(file instanceof TFile) || file.extension !== "md") {
					return false;
				}

				if (!checking) {
					void this.previewActiveNote(file);
				}

				return true;
			},
		});

		this.addCommand({
			id: "upload-active-note-to-walrus",
			name: "Upload current note to Walrus",
			checkCallback: (checking: boolean) => {
				const file = this.app.workspace.getActiveFile();
				if (!(file instanceof TFile) || file.extension !== "md") {
					return false;
				}

				if (!checking) {
					void this.uploadActiveNote();
				}

				return true;
			},
		});

		this.addCommand({
			id: "show-walrus-uploads",
			name: "Open Walrus panel",
			callback: () => {
				void this.openPluginHome();
			},
		});

		this.addCommand({
			id: "download-latest-upload-for-active-note",
			name: "Download latest uploaded version for current note",
			checkCallback: (checking: boolean) => {
				const file = this.app.workspace.getActiveFile();
				if (!(file instanceof TFile) || file.extension !== "md") {
					return false;
				}

				if (!checking) {
					void this.downloadLatestForActiveNote(file);
				}

				return true;
			},
		});

		this.addCommand({
			id: "test-sdk-connections",
			name: "Test Sui/Walrus connection",
			callback: () => {
				void this.testSdkConnections();
			},
		});
	}

	onunload(): void {
		this.app.workspace.detachLeavesOfType(WALRUS_STORAGE_VIEW_TYPE);
	}

	async loadSettings(): Promise<void> {
		this.settings = Object.assign({}, DEFAULT_SETTINGS, await this.loadData());
	}

	async saveSettings(): Promise<void> {
		await this.saveData(this.settings);
	}

	async openPluginHome(): Promise<void> {
		this.app.workspace.detachLeavesOfType(WALRUS_STORAGE_VIEW_TYPE);
		const leaf = this.app.workspace.getRightLeaf(false);
		if (!leaf) {
			new Notice("Unable to open the right panel.");
			return;
		}

		await leaf.setViewState({
			type: WALRUS_STORAGE_VIEW_TYPE,
			active: true,
		});
		await this.app.workspace.revealLeaf(leaf);
		this.refreshRightView();
	}

	openPluginSettings(): void {
		const setting = (this.app as unknown as { setting?: any }).setting;
		if (!setting) {
			new Notice("Unable to open Obsidian settings. Please open settings manually.");
			return;
		}

		if (typeof setting.open === "function") {
			setting.open();
		}

		if (typeof setting.openTabById === "function") {
			setting.openTabById(this.manifest.id);
			return;
		}

		new Notice("Unable to locate the plugin settings tab. Please select Walrus Storage in settings.");
	}

	refreshSettingTab(): void {
		if (this.settingTab) {
			this.settingTab.display();
		}
	}

	refreshRightView(): void {
		const leaves = this.app.workspace.getLeavesOfType(WALRUS_STORAGE_VIEW_TYPE);
		for (const leaf of leaves) {
			const view = leaf.view;
			if (view instanceof WalrusStorageRightView) {
				void view.render();
			}
		}
	}

	private getDownloadFolderPath(): string {
		const fallback = DEFAULT_SETTINGS.downloadFolder;
		const raw = (this.settings.downloadFolder || fallback).trim();
		const cleaned = raw.replace(/^\/+/, "").replace(/\/+$/, "");
		return normalizePath(cleaned || fallback);
	}

	async openDownloadFolder(): Promise<void> {
		const folder = this.getDownloadFolderPath();
		await this.ensureFolderExists(folder);

		try {
			const adapter = this.app.vault.adapter as any;
			const fullPath =
				typeof adapter?.getFullPath === "function" ? adapter.getFullPath(folder) : "";
			if (fullPath) {
				const electron = require("electron") as any;
				if (electron?.shell?.openPath) {
					await electron.shell.openPath(fullPath);
					return;
				}
			}
		} catch {}

		const abstract = this.app.vault.getAbstractFileByPath(folder);
		const leaf = this.app.workspace.getLeavesOfType("file-explorer")[0];
		const view = leaf?.view as any;
		if (abstract && typeof view?.revealInFolder === "function") {
			view.revealInFolder(abstract);
			return;
		}

		new Notice("Unable to open download folder.");
	}

	private initializeClients(): void {
		this.clients = new WalrusStorageClients({
			suiNetwork: this.settings.suiNetwork,
			walrusNetwork: this.settings.walrusNetwork,
		});
	}

	async refreshClients(): Promise<void> {
		this.clients.updateNetwork({
			suiNetwork: this.settings.suiNetwork,
			walrusNetwork: this.settings.walrusNetwork,
		});
	}

	private getWalrusPrivateKey(): string {
		return (
			this.settings.walrusPrivateKey.trim() || this.settings.suiPrivateKey.trim()
		);
	}

	private getWalrusSigner(): Signer {
		const privateKey = this.getWalrusPrivateKey();
		if (!privateKey) {
			throw new Error("Please set a Sui private key or a Walrus private key in settings first.");
		}

		return this.clients.createSigner(privateKey);
	}

	async deriveAddressesFromPrivateKeys(): Promise<void> {
		try {
			if (!this.settings.suiPrivateKey.trim() && !this.getWalrusPrivateKey()) {
				throw new Error("Please provide at least one private key first.");
			}

			if (this.settings.suiPrivateKey.trim()) {
				this.settings.suiAddress = this.clients.getAddressFromPrivateKey(
					this.settings.suiPrivateKey,
				);
			}

			const walrusPrivateKey = this.getWalrusPrivateKey();
			if (walrusPrivateKey) {
				this.settings.walrusAddress =
					this.clients.getAddressFromPrivateKey(walrusPrivateKey);
			}

			await this.saveSettings();
			this.refreshSettingTab();
			this.refreshRightView();
			new Notice("Addresses derived from private key.");
		} catch (error) {
			new Notice(this.formatError(error, "Failed to derive addresses"));
		}
	}

	async initializeSuiFromMnemonic(
		mnemonics: string,
		path?: string,
	): Promise<void> {
		try {
			const keypair = this.clients.deriveEd25519KeypairFromMnemonic(
				mnemonics,
				path,
			);
			this.settings.suiPrivateKey = keypair.getSecretKey();
			await this.deriveAddressesFromPrivateKeys();
			new Notice("Sui address initialized from mnemonic.");
		} catch (error) {
			new Notice(this.formatError(error, "Failed to initialize from mnemonic"));
		}
	}

	async testSdkConnections(): Promise<void> {
		new Notice("Testing Sui/Walrus connection...");
		const results = await this.clients.testConnection();

		new Notice(
			`Sui: ${results.sui ? "OK" : "FAILED"} | Walrus: ${results.walrus ? "OK" : "FAILED"}`,
		);
	}

	async uploadActiveNote(): Promise<void> {
		try {
			const traceId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
			const file = this.app.workspace.getActiveFile();
			if (!(file instanceof TFile) || file.extension !== "md") {
				throw new Error("Please open a Markdown note first.");
			}

			const content = await this.app.vault.read(file);
			if (!content.trim()) {
				throw new Error("Current note is empty.");
			}

			const signer = this.getWalrusSigner();
			// #region debug-point D:upload-start
			(() => {
				const u = "http://127.0.0.1:7777/event";
				const s = "upload-fails-balances";
				fetch(u, {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						sessionId: s,
						runId: "post-fix",
						hypothesisId: "D",
						location: "main.ts:uploadActiveNote",
						traceId,
						msg: "[DEBUG] upload start",
						data: {
							filePath: file.path,
							fileName: file.name,
							contentLength: content.length,
							suiNetwork: this.settings.suiNetwork,
							walrusNetwork: this.settings.walrusNetwork,
							epochs: this.settings.storageEpochs,
							signerAddress: signer.toSuiAddress(),
							hasSuiAddress: Boolean(this.settings.suiAddress),
							hasWalrusAddress: Boolean(this.settings.walrusAddress),
						},
						ts: Date.now(),
					}),
				}).catch(() => {});
			})();
			// #endregion

			if (!this.settings.suiAddress || !this.settings.walrusAddress) {
				await this.deriveAddressesFromPrivateKeys();
			}

			// #region debug-point A:balances-before-upload
			(() => {
				const u = "http://127.0.0.1:7777/event";
				const s = "upload-fails-balances";
				this.clients
					.getSuiClient()
					.getAllBalances({ owner: signer.toSuiAddress() })
					.then((balances: unknown) => {
						fetch(u, {
							method: "POST",
							headers: { "Content-Type": "application/json" },
							body: JSON.stringify({
								sessionId: s,
								runId: "post-fix",
								hypothesisId: "A",
								location: "main.ts:balances",
								traceId,
								msg: "[DEBUG] balances snapshot",
								data: { balances },
								ts: Date.now(),
							}),
						}).catch(() => {});
					})
					.catch((err: unknown) => {
						fetch(u, {
							method: "POST",
							headers: { "Content-Type": "application/json" },
							body: JSON.stringify({
								sessionId: s,
								runId: "post-fix",
								hypothesisId: "A",
								location: "main.ts:balances",
								traceId,
								msg: "[DEBUG] balances snapshot failed",
								data: {
									error:
										err instanceof Error
											? { name: err.name, message: err.message, stack: err.stack }
											: { value: String(err) },
								},
								ts: Date.now(),
							}),
						}).catch(() => {});
					});
			})();
			// #endregion

			new Notice("Uploading current note to Walrus...");

			const encoder = new TextEncoder();
			const result = await this.clients.uploadBlob({
				blob: encoder.encode(content),
				epochs: Math.max(1, this.settings.storageEpochs),
				signer,
				owner: signer.toSuiAddress(),
				attributes: {
					fileName: file.name,
					sourcePath: file.path,
					uploadedAt: new Date().toISOString(),
				},
			});

			this.upsertUploadRecord({
				blobId: result.blobId,
				blobObjectId: result.blobObjectId,
				sourcePath: file.path,
				sourceName: file.name,
				size: result.size,
				uploadedAt: new Date().toISOString(),
				suiAddress: signer.toSuiAddress(),
				suiNetwork: this.settings.suiNetwork,
				walrusNetwork: this.settings.walrusNetwork,
			});

			await this.saveSettings();
			this.refreshSettingTab();
			this.refreshRightView();

			new Notice(`Upload succeeded. Blob ID: ${result.blobId}`);
		} catch (error) {
			// #region debug-point E:upload-error
			(() => {
				const u = "http://127.0.0.1:7777/event";
				const s = "upload-fails-balances";
				fetch(u, {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						sessionId: s,
						runId: "post-fix",
						hypothesisId: "E",
						location: "main.ts:uploadActiveNote:catch",
						msg: "[DEBUG] upload failed",
						data: {
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
			new Notice(this.formatError(error, "Upload failed"));
		}
	}

	async downloadLatestForActiveNote(file: TFile): Promise<void> {
		const matchedRecords = this.settings.uploads
			.filter((record) => record.sourcePath === file.path)
			.sort((left, right) => right.uploadedAt.localeCompare(left.uploadedAt));

		if (matchedRecords.length === 0) {
			new Notice("No uploaded version found for the current note.");
			return;
		}

		await this.downloadRecord(matchedRecords[0]);
	}

	async downloadRecord(record: WalrusUploadRecord): Promise<string> {
		new Notice(`Downloading ${record.sourceName}...`);

		try {
			let bytes: Uint8Array;
			try {
				bytes = await this.clients.downloadBlob(record.blobId);
			} catch (primaryError) {
				bytes = await this.clients.downloadBlobByObjectId(record.blobObjectId).catch(
					(fallbackError: unknown) => {
						const primaryMessage = this.formatError(
							primaryError,
							"Blob ID download failed",
						);
						const fallbackMessage = this.formatError(
							fallbackError,
							"Object ID fallback download failed",
						);
						throw new Error(`${primaryMessage}; ${fallbackMessage}`);
					},
				);
			}
			const content = new TextDecoder().decode(bytes);
			const targetPath = this.buildDownloadPath(record);

			await this.ensureFolderExists(this.getDownloadFolderPath());
			const existingFile = this.app.vault.getAbstractFileByPath(targetPath);

			if (existingFile instanceof TFile) {
				await this.app.vault.modify(existingFile, content);
			} else {
				await this.app.vault.create(targetPath, content);
			}

			this.refreshRightView();
			new Notice(`Downloaded to ${targetPath}`);
			this.openDownloadModal({ success: true, vaultPath: targetPath });
			return targetPath;
		} catch (error) {
			const message = this.formatError(error, "Download failed");
			new Notice(message);
			this.openDownloadModal({ success: false, vaultPath: record.sourceName, error: message });
			throw error;
		}
	}

	async downloadBlobToVault(blobId: string, blobObjectId?: string): Promise<string> {
		new Notice(`Downloading blob ${blobId}...`);

		try {
			let bytes: Uint8Array;
			try {
				bytes = await this.clients.downloadBlob(blobId);
			} catch (primaryError) {
				if (!blobObjectId) {
					throw primaryError;
				}

				bytes = await this.clients.downloadBlobByObjectId(blobObjectId).catch(
					(fallbackError: unknown) => {
						const primaryMessage = this.formatError(
							primaryError,
							"Blob ID download failed",
						);
						const fallbackMessage = this.formatError(
							fallbackError,
							"Object ID fallback download failed",
						);
						throw new Error(`${primaryMessage}; ${fallbackMessage}`);
					},
				);
			}
			const suffix = blobId.slice(0, 8);
			const folder = this.getDownloadFolderPath();
			const targetPath = normalizePath(
				`${folder}/blob-${suffix}.bin`,
			);
			const buffer = bytes.slice().buffer;

			await this.ensureFolderExists(folder);
			const existingFile = this.app.vault.getAbstractFileByPath(targetPath);

			if (existingFile instanceof TFile) {
				await this.app.vault.modifyBinary(existingFile, buffer);
			} else {
				await this.app.vault.createBinary(targetPath, buffer);
			}

			this.refreshRightView();
			new Notice(`Downloaded to ${targetPath}`);
			this.openDownloadModal({ success: true, vaultPath: targetPath });
			return targetPath;
		} catch (error) {
			const message = this.formatError(error, "Download failed");
			new Notice(message);
			this.openDownloadModal({ success: false, vaultPath: blobId, error: message });
			throw error;
		}
	}

	private openDownloadModal(payload: {
		success: boolean;
		vaultPath: string;
		error?: string;
	}): void {
		const fullPath = payload.success ? this.getFullPath(payload.vaultPath) : "";
		try {
			new DownloadResultModal(this.app, {
				success: payload.success,
				vaultPath: payload.vaultPath,
				fullPath,
				error: payload.error || "",
			}).open();
		} catch {}
	}

	private getFullPath(vaultPath: string): string {
		try {
			const adapter = this.app.vault.adapter as any;
			if (typeof adapter?.getFullPath === "function") {
				return String(adapter.getFullPath(vaultPath) ?? "");
			}
		} catch {}
		return "";
	}

	async getOwnedBlobs(): Promise<OwnedBlobRecord[]> {
		const address = await this.ensureSuiAddress();
		return this.clients.listOwnedBlobs(address);
	}

	async getCurrentBalances(): Promise<TokenBalanceSummary> {
		const address = await this.ensureSuiAddress();
		return this.clients.getAddressBalances(address);
	}

	private async ensureSuiAddress(): Promise<string> {
		if (!this.settings.suiAddress.trim()) {
			await this.deriveAddressesFromPrivateKeys();
		}

		if (!this.settings.suiAddress.trim()) {
			throw new Error("Missing Sui address. Please set a private key and derive addresses first.");
		}

		return this.settings.suiAddress.trim();
	}

	private buildDownloadPath(record: WalrusUploadRecord): string {
		const suffix = record.blobId.slice(0, 8);
		const baseName = this.getBaseName(record.sourceName);
		const folder = this.getDownloadFolderPath();
		return normalizePath(
			`${folder}/${baseName}-${suffix}.md`,
		);
	}

	private getBaseName(fileName: string): string {
		const lastDotIndex = fileName.lastIndexOf(".");
		return lastDotIndex > 0 ? fileName.slice(0, lastDotIndex) : fileName;
	}

	private async ensureFolderExists(folderPath: string): Promise<void> {
		const normalizedFolder = normalizePath(folderPath.trim());
		if (!normalizedFolder || normalizedFolder === ".") {
			return;
		}

		const segments = normalizedFolder.split("/").filter(Boolean);
		let currentPath = "";

		for (const segment of segments) {
			currentPath = currentPath ? `${currentPath}/${segment}` : segment;
			if (!this.app.vault.getAbstractFileByPath(currentPath)) {
				await this.app.vault.createFolder(currentPath);
			}
		}
	}

	private upsertUploadRecord(record: WalrusUploadRecord): void {
		const uploads = this.settings.uploads.filter(
			(item) => item.blobId !== record.blobId,
		);
		uploads.unshift(record);
		this.settings.uploads = uploads;
	}

	private async previewActiveNote(file: TFile): Promise<void> {
		const content = await this.app.vault.read(file);
		const preview =
			content.trim().length > 120
				? `${content.trim().slice(0, 120)}...`
				: content.trim();

		if (!preview) {
			new Notice("Current note is empty.");
			return;
		}

		new Notice(`Preview: ${preview}`);
	}

	formatError(error: unknown, fallback: string): string {
		if (error instanceof Error && error.message) {
			return `${fallback}: ${error.message}`;
		}

		return fallback;
	}
}

class WalrusStorageSettingTab extends PluginSettingTab {
	plugin: WalrusStoragePlugin;
	private mnemonicDraft = "";
	private mnemonicPathDraft = "";

	constructor(app: App, plugin: WalrusStoragePlugin) {
		super(app, plugin);
		this.plugin = plugin;
	}

	display(): void {
		const { containerEl } = this;
		containerEl.empty();

		containerEl.createEl("h2", { text: "Walrus Storage Settings" });
		containerEl.createEl("p", {
			text: "This version supports address derivation, uploading the current note, viewing upload records, and downloading.",
		});

		new Setting(containerEl)
			.setName("Sui network")
			.setDesc("Select the Sui network.")
			.addDropdown((dropdown: DropdownComponent) => {
				dropdown
					.addOption("testnet", "Testnet")
					.addOption("mainnet", "Mainnet")
					.setValue(this.plugin.settings.suiNetwork)
					.onChange(async (value: string) => {
						this.plugin.settings.suiNetwork =
							value === "mainnet" ? "mainnet" : "testnet";
						await this.plugin.saveSettings();
						await this.plugin.refreshClients();
					});
			});

		new Setting(containerEl)
			.setName("Walrus network")
			.setDesc("Select the Walrus network.")
			.addDropdown((dropdown: DropdownComponent) => {
				dropdown
					.addOption("testnet", "Testnet")
					.addOption("mainnet", "Mainnet")
					.setValue(this.plugin.settings.walrusNetwork)
					.onChange(async (value: string) => {
						this.plugin.settings.walrusNetwork =
							value === "mainnet" ? "mainnet" : "testnet";
						await this.plugin.saveSettings();
						await this.plugin.refreshClients();
					});
			});

		new Setting(containerEl)
			.setName("Storage epochs")
			.setDesc("Epochs to use when uploading to Walrus.")
			.addText((text: TextComponent) => {
				text
					.setPlaceholder("5")
					.setValue(String(this.plugin.settings.storageEpochs))
					.onChange(async (value: string) => {
						const parsedValue = Number.parseInt(value || "0", 10);
						this.plugin.settings.storageEpochs =
							Number.isFinite(parsedValue) && parsedValue > 0 ? parsedValue : 1;
						await this.plugin.saveSettings();
					});
			});

		new Setting(containerEl)
			.setName("Download folder")
			.setDesc("Downloaded files will be written to this folder.")
			.addText((text: TextComponent) => {
				text
					.setPlaceholder("walrus-downloads")
					.setValue(this.plugin.settings.downloadFolder)
					.onChange(async (value: string) => {
						const sanitized = value.trim().replace(/^\/+/, "").replace(/\/+$/, "");
						this.plugin.settings.downloadFolder =
							normalizePath(sanitized || "walrus-downloads");
						await this.plugin.saveSettings();
					});
			});

		new Setting(containerEl)
			.setName("Test SDK connection")
			.setDesc("Initialize clients with current network config and run a connectivity test.")
			.addButton((button) => {
				button.setButtonText("Test").onClick(() => {
					void this.plugin.testSdkConnections();
				});
			});

		new Setting(containerEl)
			.setName("Derive addresses")
			.setDesc("Derive Sui/Walrus addresses from private key.")
			.addButton((button) => {
				button.setButtonText("Derive").onClick(() => {
					void this.plugin.deriveAddressesFromPrivateKeys();
				});
			});

		new Setting(containerEl)
			.setName("Initialize from mnemonic (Sui)")
			.setDesc("Derive suiprivkey and addresses from mnemonic. The mnemonic is not stored; only the derived suiprivkey is saved.")
			.addTextArea((textArea: TextAreaComponent) => {
				textArea
					.setPlaceholder("word1 word2 ...")
					.setValue(this.mnemonicDraft)
					.onChange((value: string) => {
						this.mnemonicDraft = value;
					});
				textArea.inputEl.rows = 3;
			})
			.addText((text: TextComponent) => {
				text
					.setPlaceholder("Derivation path (optional)")
					.setValue(this.mnemonicPathDraft)
					.onChange((value: string) => {
						this.mnemonicPathDraft = value;
					});
			})
			.addButton((button) => {
				button.setButtonText("Initialize").onClick(() => {
					const mnemonics = this.mnemonicDraft;
					const path = this.mnemonicPathDraft;
					this.mnemonicDraft = "";
					this.mnemonicPathDraft = "";
					void this.plugin.initializeSuiFromMnemonic(mnemonics, path);
					this.display();
				});
			});

		new Setting(containerEl)
			.setName("Sui address")
			.setDesc("Current Sui address.")
			.addText((text: TextComponent) => {
				text
					.setPlaceholder("0x...")
					.setValue(this.plugin.settings.suiAddress)
					.onChange(async (value: string) => {
						this.plugin.settings.suiAddress = value.trim();
						await this.plugin.saveSettings();
					});
			});

		new Setting(containerEl)
			.setName("Walrus address")
			.setDesc("Address used for Walrus writes. Defaults to reusing the Sui private key.")
			.addText((text: TextComponent) => {
				text
					.setPlaceholder("0x...")
					.setValue(this.plugin.settings.walrusAddress)
					.onChange(async (value: string) => {
						this.plugin.settings.walrusAddress = value.trim();
						await this.plugin.saveSettings();
					});
			});

		new Setting(containerEl)
			.setName("Sui private key")
			.setDesc("Used to derive Sui address and sign transactions.")
			.addText((text: TextComponent) => {
				text
					.setPlaceholder("suiprivkey...")
					.setValue(this.plugin.settings.suiPrivateKey)
					.onChange(async (value: string) => {
						this.plugin.settings.suiPrivateKey = value.trim();
						await this.plugin.saveSettings();
					});

				text.inputEl.type = "password";
			});

		new Setting(containerEl)
			.setName("Walrus private key")
			.setDesc("Optional. If empty, the Sui private key will be used.")
			.addText((text: TextComponent) => {
				text
					.setPlaceholder("suiprivkey...")
					.setValue(this.plugin.settings.walrusPrivateKey)
					.onChange(async (value: string) => {
						this.plugin.settings.walrusPrivateKey = value.trim();
						await this.plugin.saveSettings();
					});

				text.inputEl.type = "password";
			});

		new Setting(containerEl)
			.setName("Upload")
			.setDesc("Upload the currently opened Markdown note to Walrus.")
			.addButton((button) => {
				button.setButtonText("Upload current note").onClick(() => {
					void this.plugin.uploadActiveNote();
				});
			});

		new Setting(containerEl)
			.setName("Open panel")
			.setDesc(`Local upload records: ${this.plugin.settings.uploads.length}`)
			.addButton((button) => {
				button.setButtonText("Open").onClick(() => {
					void this.plugin.openPluginHome();
				});
			});
	}
}

class WalrusStorageRightView extends ItemView {
	plugin: WalrusStoragePlugin;
	private actionsAdded = false;
	private localUploadsCollapsed = false;
	private onChainBlobsCollapsed = false;

	constructor(leaf: WorkspaceLeaf, plugin: WalrusStoragePlugin) {
		super(leaf);
		this.plugin = plugin;
	}

	getViewType(): string {
		return WALRUS_STORAGE_VIEW_TYPE;
	}

	getDisplayText(): string {
		return "Walrus Storage";
	}

	getIcon(): string {
		return "database";
	}

	async onOpen(): Promise<void> {
		if (!this.actionsAdded) {
			this.addAction("gear", "Open settings", () => {
				this.plugin.openPluginSettings();
			});
			this.addAction("refresh-cw", "Refresh panel", () => {
				void this.render();
			});
			this.actionsAdded = true;
		}

		await this.render();
	}

	async render(): Promise<void> {
		this.contentEl.empty();

		await this.renderBalanceSummary();
		this.renderQuickActions();
		this.renderLocalUploads();
		await this.renderOwnedBlobs();
	}

	async onClose(): Promise<void> {
		this.contentEl.empty();
	}

	private renderQuickActions(): void {
		this.contentEl.createEl("h3", { text: "Quick Actions" });

		const actions = this.contentEl.createDiv();
		const createIconButton = (
			icon: string,
			label: string,
			onClick: () => void,
		): void => {
			const button = actions.createEl("button");
			button.setAttribute("aria-label", label);
			button.setAttribute("title", label);
			setIcon(button, icon);
			button.onclick = onClick;
		};

		createIconButton("gear", "Open settings", () => {
			this.plugin.openPluginSettings();
		});
		createIconButton("key", "Derive addresses", () => {
			void this.plugin.deriveAddressesFromPrivateKeys();
		});
		createIconButton("link", "Test connection", () => {
			void this.plugin.testSdkConnections();
		});
		createIconButton("upload", "Upload current note", () => {
			void this.plugin.uploadActiveNote();
		});
		createIconButton("folder-open", "Open download folder", () => {
			void this.plugin.openDownloadFolder();
		});
	}

	private async renderBalanceSummary(): Promise<void> {
		const header = this.contentEl.createDiv();
		header.createEl("h3", { text: "Balances" });
		const refreshButton = header.createEl("button");
		refreshButton.setAttribute("aria-label", "Refresh balances");
		refreshButton.setAttribute("title", "Refresh balances");
		setIcon(refreshButton, "refresh-cw");
		refreshButton.onclick = () => {
			void this.render();
		};

		const addressText = this.plugin.settings.suiAddress.trim();
		if (!addressText) {
			this.contentEl.createEl("p", {
				text: "No address yet. Please derive addresses in settings first.",
			});
			return;
		}

		this.contentEl.createEl("p", {
			text: `Address: ${addressText}`,
		});

		try {
			const balances = await this.plugin.getCurrentBalances();
			this.renderBalanceItem("SUI", balances.sui.formatted, balances.sui.totalBalance);
			this.renderBalanceItem("WAL", balances.wal.formatted, balances.wal.totalBalance);
		} catch (error) {
			this.contentEl.createEl("p", {
				text: this.plugin.formatError(error, "Failed to fetch balances"),
			});
		}
	}

	private renderBalanceItem(symbol: string, amount: string, rawAmount: string): void {
		const row = this.contentEl.createDiv();
		row.createEl("strong", { text: `${symbol}: ` });
		row.createSpan({ text: amount });
		row.createEl("small", { text: ` (${rawAmount})` });
	}

	private setCollapseToggleIcon(button: HTMLElement, collapsed: boolean): void {
		setIcon(button, collapsed ? "chevron-right" : "chevron-down");
	}

	private renderLocalUploads(): void {
		const header = this.contentEl.createDiv();
		const title = header.createEl("h3", { text: "Local Uploads" });
		const toggleButton = header.createEl("button");
		toggleButton.setAttribute(
			"aria-label",
			this.localUploadsCollapsed ? "Expand local uploads" : "Collapse local uploads",
		);
		toggleButton.setAttribute(
			"title",
			this.localUploadsCollapsed ? "Expand local uploads" : "Collapse local uploads",
		);
		this.setCollapseToggleIcon(toggleButton, this.localUploadsCollapsed);

		const body = this.contentEl.createDiv();
		body.style.display = this.localUploadsCollapsed ? "none" : "";

		toggleButton.onclick = () => {
			this.localUploadsCollapsed = !this.localUploadsCollapsed;
			body.style.display = this.localUploadsCollapsed ? "none" : "";
			toggleButton.setAttribute(
				"aria-label",
				this.localUploadsCollapsed ? "Expand local uploads" : "Collapse local uploads",
			);
			toggleButton.setAttribute(
				"title",
				this.localUploadsCollapsed ? "Expand local uploads" : "Collapse local uploads",
			);
			this.setCollapseToggleIcon(toggleButton, this.localUploadsCollapsed);
		};

		const uploadCount = this.plugin.settings.uploads.length;
		title.setText(`Local Uploads (${uploadCount})`);

		if (this.plugin.settings.uploads.length === 0) {
			body.createEl("p", { text: "No local upload records yet." });
			return;
		}

		for (const record of this.plugin.settings.uploads) {
			const row = body.createDiv();
			row.createEl("strong", { text: record.sourceName });
			row.createEl("div", {
				text: `Blob ID: ${record.blobId}`,
			});
			row.createEl("div", {
				text: `Uploaded at: ${record.uploadedAt}`,
			});
			row.createEl("div", {
				text: `Network: Sui ${record.suiNetwork} / Walrus ${record.walrusNetwork}`,
			});

			const downloadButton = row.createEl("button", {
				text: "Download",
			});
			downloadButton.onclick = () => {
				void this.plugin.downloadRecord(record);
			};
		}
	}

	private async renderOwnedBlobs(): Promise<void> {
		const header = this.contentEl.createDiv();
		const title = header.createEl("h3", { text: "On-chain Blobs" });
		const toggleButton = header.createEl("button");
		toggleButton.setAttribute(
			"aria-label",
			this.onChainBlobsCollapsed ? "Expand on-chain blobs" : "Collapse on-chain blobs",
		);
		toggleButton.setAttribute(
			"title",
			this.onChainBlobsCollapsed ? "Expand on-chain blobs" : "Collapse on-chain blobs",
		);
		this.setCollapseToggleIcon(toggleButton, this.onChainBlobsCollapsed);

		const body = this.contentEl.createDiv();
		body.style.display = this.onChainBlobsCollapsed ? "none" : "";

		toggleButton.onclick = () => {
			this.onChainBlobsCollapsed = !this.onChainBlobsCollapsed;
			body.style.display = this.onChainBlobsCollapsed ? "none" : "";
			toggleButton.setAttribute(
				"aria-label",
				this.onChainBlobsCollapsed ? "Expand on-chain blobs" : "Collapse on-chain blobs",
			);
			toggleButton.setAttribute(
				"title",
				this.onChainBlobsCollapsed ? "Expand on-chain blobs" : "Collapse on-chain blobs",
			);
			this.setCollapseToggleIcon(toggleButton, this.onChainBlobsCollapsed);
		};

		try {
			const blobs = await this.plugin.getOwnedBlobs();
			title.setText(`On-chain Blobs (${blobs.length})`);
			if (blobs.length === 0) {
				body.createEl("p", {
					text: "No on-chain blob objects found for this address.",
				});
				return;
			}

			for (const blob of blobs) {
				const row = body.createDiv();
				row.createEl("div", { text: `Blob ID: ${blob.blobId}` });
				row.createEl("div", { text: `Object ID: ${blob.blobObjectId}` });
				row.createEl("div", {
					text: `Size: ${blob.size ?? "Unknown"} bytes`,
				});
				const downloadButton = row.createEl("button");
				downloadButton.setAttribute("aria-label", "Download blob");
				downloadButton.setAttribute("title", "Download");
				setIcon(downloadButton, "download");
				downloadButton.onclick = () => {
					void this.plugin.downloadBlobToVault(blob.blobId, blob.blobObjectId);
				};
			}
		} catch (error) {
			body.createEl("p", {
				text: this.plugin.formatError(error, "Failed to fetch on-chain blobs"),
			});
		}
	}
}

class DownloadResultModal extends Modal {
	private result: { success: boolean; vaultPath: string; fullPath: string; error: string };

	constructor(
		app: App,
		result: { success: boolean; vaultPath: string; fullPath: string; error: string },
	) {
		super(app);
		this.result = result;
	}

	onOpen(): void {
		const { contentEl } = this;
		contentEl.empty();

		contentEl.createEl("h3", {
			text: this.result.success ? "Download complete" : "Download failed",
		});
		contentEl.createEl("p", { text: `Vault path: ${this.result.vaultPath}` });
		if (this.result.fullPath) {
			contentEl.createEl("p", { text: `Full path: ${this.result.fullPath}` });
		}
		if (!this.result.success && this.result.error) {
			contentEl.createEl("p", { text: this.result.error });
		}

		const actions = contentEl.createDiv();
		if (this.result.fullPath) {
			const showButton = actions.createEl("button", { text: "Show in folder" });
			showButton.onclick = () => {
				try {
					const electron = require("electron") as any;
					if (electron?.shell?.showItemInFolder && this.result.fullPath) {
						electron.shell.showItemInFolder(this.result.fullPath);
					} else if (electron?.shell?.openPath && this.result.fullPath) {
						void electron.shell.openPath(this.result.fullPath);
					}
				} catch {}
				this.close();
			};
		}

		const closeButton = actions.createEl("button", { text: "Close" });
		closeButton.onclick = () => {
			this.close();
		};
	}
}
