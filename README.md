# Walrus Storage for Obsidian

A Walrus storage plugin for Obsidian — upload, manage, and download your notes on the Walrus decentralized storage network.

## Features

- **Right-side panel** with balance display, quick actions, upload history, and on-chain blob management
- **Upload** current Markdown notes to Walrus (testnet / mainnet)
- **Download** blobs back to your vault with success/failure feedback
- **Address management** via private key or mnemonic derivation
- **SUI & WAL balance** display with live refresh
- **Multi-aggregator** fallback for blob downloads

## How to Install

### Build from Source

```bash
# Clone the repository
git clone <repository-url>
cd walus_storage_obsidian

# Install dependencies
npm install

# Build the plugin
npm run build
```

### Copy to Obsidian Vault

After building, copy these files into your Obsidian vault's plugin directory:

```bash
# Create plugin directory
mkdir -p "<YOUR_VAULT>/.obsidian/plugins/walus-storage-obsidian"

# Copy plugin files
cp main.js manifest.json versions.json "<YOUR_VAULT>/.obsidian/plugins/walus-storage-obsidian/"
```

### Using the Sync Script

You can also use the included sync script to build and deploy automatically:

```bash
bash scripts/sync-obsidian-plugin.sh
```

This builds the plugin and copies files to `~/Documents/Obsidian Vault/.obsidian/plugins/walus-storage-obsidian/` by default. You can specify a custom target path:

```bash
bash scripts/sync-obsidian-plugin.sh "<YOUR_VAULT>/.obsidian/plugins/walus-storage-obsidian"
```

### Enable the Plugin

1. Open Obsidian → **Settings** → **Community plugins**
2. Find **Walus Storage** in the installed plugins list
3. Toggle it **ON**

## How to Use

### 1. Configure Settings

Open **Settings** → **Walus Storage** and set:

- **Sui network**: Select `Testnet` or `Mainnet`
- **Walrus network**: Select `Testnet` or `Mainnet`
- **Storage epochs**: Number of epochs for blob storage (default: 5)
- **Download folder**: Vault folder for downloaded files (default: `walrus-downloads`)

### 2. Initialize Your Wallet

Choose one of the following methods:

**Option A — From Mnemonic:**
1. In Settings → **Initialize from mnemonic (Sui)**, enter your 12/24-word mnemonic phrase
2. (Optional) Set a custom derivation path
3. Click **Initialize**

**Option B — From Private Key:**
1. In Settings → **Sui private key**, paste your `suiprivkey...` key
2. Click **Derive** to generate addresses

### 3. Test Connection

In Settings, click **Test** under "Test SDK connection" to verify Sui and Walrus connectivity.

### 4. Open the Right Panel

- Click the **database** icon in the left sidebar ribbon, or
- Use the command palette: **Open Walrus panel**

The right panel displays:

- **Balances** — SUI and WAL token balances with refresh button
- **Quick Actions** — Icon buttons for common operations:
  - ⚙️ Open settings
  - 🔑 Derive addresses
  - 🔗 Test connection
  - ⬆️ Upload current note
  - 📁 Open download folder
- **Local Uploads** — Your upload history with download buttons
- **On-chain Blobs** — Blobs owned by your address on-chain, with download buttons

### 5. Upload a Note

1. Open a Markdown note in the editor
2. Click the ⬆️ icon in Quick Actions, or use the command palette: **Upload current note to Walrus**
3. The blob will be stored on Walrus and recorded in Local Uploads

### 6. Download a Blob

- From **Local Uploads**: click the **Download** button on any record
- From **On-chain Blobs**: click the ⬇️ icon on any blob
- A modal will show the download result with the vault path and full file path
- Use the 📁 Quick Action to open the download folder

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Run sync script
bash scripts/sync-obsidian-plugin.sh
```



