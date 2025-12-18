# 📸 Snapchat Memories Downloader

> **Download all your Snapchat memories from your data export — photos, videos, and filters.**

This tool helps you download your Snapchat memories to your computer. It works on **Windows**, **Mac**, and **Linux**.

---
#### ❤️ If you wanna support me (FREE):

> **check out my music** (mostly dnb) **and possible some other links in the future:** [Linktree](https://linktr.ee/sslax)

---

## ⚠️ Snapchat Is Deleting Your Memories — Act Now!

**In late 2025, Snapchat introduced a 5GB storage cap on Memories.** If you have more than 5GB of memories saved (which many long-time users do), you now have **two choices**:

1. 💸 **Pay $2-16/month** for additional storage
2. 😢 **Lose your memories forever** after a 12-month grace period

> **For users who've been on Snapchat for years — sometimes a decade — this means childhood photos, special moments, and irreplaceable memories could be permanently deleted.**

Snapchat has accumulated over **1 trillion saved Snaps** since Memories launched in 2016. Many users feel blindsided by this sudden monetization of what was once free unlimited storage.

### What happens if you don't act?

After the 12-month grace period, **Snapchat will automatically delete** your most recent memories that exceed the 5GB limit. There's no recovery option.

### The solution: Download your memories now

**This tool lets you save ALL your Snapchat memories to your own computer** — for free, forever. No monthly fees. No risk of deletion.

---

## 🤔 Why This Tool?

**Snapchat's built-in download is broken.** If you've tried downloading your memories directly from Snapchat, you've probably experienced:

| ❌ Snapchat's Download | ✅ This Tool |
|------------------------|--------------|
| Doesn't work most of the time | Works more reliable (create issue when it doesn't) |
| Extremely slow downloads | Fast parallel downloads |
| Random, meaningless filenames | Organized by date & time |
| No folder structure (all files dumped together) | Sorted into Year/Month/Day folders |
| Missing file extensions (can't open files!) | Correct extensions (.jpg, .mp4, .png, .zip) |
| No creation timestamps | Preserves original date/time in filename |
| GPS coordinates lost | Keeps latitude/longitude in filename when available |
| Some file types completely missing | Downloads everything: photos, videos, filters |

**This tool fixes all of that.** Your memories are downloaded with proper names, in organized folders, with all metadata preserved.

---

## 🔐 Safe & Private

**Your data stays yours.** This tool is:

- ✅ **100% Open Source** — All code is visible, nothing hidden
- ✅ **Fully Transparent** — You can read exactly what every script does
- ✅ **No Data Collection** — Nothing is sent anywhere except to Snapchat's own servers
- ✅ **Uses Official Links** — Only downloads from URLs that Snapchat generated for YOU
- ✅ **Runs Locally** — Everything happens on your computer, nowhere else

> **🤝 Promise:** Your files go directly from Snapchat's servers to your computer. No middleman, no tracking, no data harvesting. Ever.

---

## 📋 Table of Contents

1. [⚠️ Why You Need This Tool NOW](#️-snapchat-is-deleting-your-memories--act-now)
2. [How It Works](#how-it-works)
3. [What You Need](#what-you-need)
4. [Get Your Snapchat Data](#get-your-snapchat-data)
5. [Setup Instructions](#setup-instructions)
   - [Windows](#windows-instructions)
   - [Mac](#mac-instructions)
   - [Linux](#linux-instructions)
6. [Configuration Options](#configuration-options)
7. [Where Are My Files?](#where-are-my-files)
8. [Stopping and Resuming](#stopping-and-resuming)
9. [Troubleshooting](#troubleshooting)
10. [FAQ](#faq)

---

## How It Works

The tool works in **2 simple steps**:

```
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│                     │      │                     │      │                     │
│  Snapchat HTML      │ ───► │   CSV File with     │ ───► │  Your Downloaded    │
│  Export File        │      │   Download Links    │      │  Photos & Videos    │
│                     │      │                     │      │                     │
└─────────────────────┘      └─────────────────────┘      └─────────────────────┘
     Your Data                   Step 1 creates            Step 2 downloads
     from Snapchat               this for you              everything
```

**Step 1:** Reads your Snapchat HTML file and creates a list of download links  
**Step 2:** Downloads all your photos and videos using that list

---

## What You Need

### Windows
- **Windows 10** or **Windows 11** (nothing extra needed!)
- That's it! Everything else is already installed on your computer.

### Mac
- **macOS 10.15** (Catalina) or newer
- The built-in Terminal app
- That's it! Everything else is already installed on your Mac.

### Linux
- **Any modern Linux distribution** (Ubuntu, Debian, Fedora, etc.)
- Bash shell (you already have this)
- curl (usually pre-installed, if not: `sudo apt install curl`)

---

## Get Your Snapchat Data

Before using this tool, you need to request your **Memories export** from Snapchat. This is different from a regular data export!

> **⏱️ How long does this take?** For ~40,000 images, expect about 1-3 hours. Larger exports may take up to a few days.

---

### Step 1: Go to Snapchat's Account Page
1. Open your web browser
2. Go to: **https://accounts.snapchat.com**
3. Log in with your Snapchat username and password

> **💡 Alternative:** You can also access this from the Snapchat app: **Settings → My Data**

---

### Step 2: Request Your Memories Export

This is the important part — you must specifically enable **Memories export**:

1. Click **"My Data"** in the menu
2. Under **"Select data to include"**, find and **turn ON the toggle** for:
   
   ✅ **"Export your Memories"**
   
3. Click **"Request Only Memories"** (or "Submit Request" if you want all data)
4. Select your **date range**:
   - Choose **"All time"** to get everything
   - Or select a specific date range
5. **Confirm your email address** where you'll receive the download link
6. Click **"Submit"**

> **⚠️ Important:** If you don't enable "Export your Memories", you won't get the HTML file needed for this tool!

---

### Step 3: Wait for the Email

1. Snapchat will prepare your export (this takes time!)
   - Small exports: ~30 minutes to 1 hour
   - Large exports (40,000+ memories): 1-3 hours
   - Very large exports: up to a few days
2. You'll receive an email when it's ready

---

### Step 4: Download Your Data

1. Open the email from Snapchat
2. Click the download link in the email
3. You'll be taken to a page — click **"See exports"**
4. Click **"Download"** to get your files as a ZIP folder

---

### Step 5: Find the Important File

1. **Extract (unzip) the downloaded ZIP file**
   - **Windows:** Right-click the ZIP → click **"Extract All"** → click **"Extract"**
   - **Mac:** Double-click the ZIP file
   - **Linux:** Right-click → **"Extract Here"**

2. Open the extracted folder and look for:
   ```
   📁 your_export_folder/
   └── 📁 html/
       └── 📄 memories_history.html  ← THIS IS THE FILE YOU NEED!
   ```

3. The file **`memories_history.html`** is what you'll use with this tool

> **💡 Tip:** Copy `memories_history.html` to your Desktop for easy access.

---

## Setup Instructions

### Windows Instructions

#### Step 1: Download the Tool
1. Click the green **"Code"** button on this page
2. Click **"Download ZIP"**
3. Extract the ZIP file to your Desktop

#### Step 2: Open PowerShell
1. Press the **Windows key** on your keyboard
2. Type **`PowerShell`**
3. Click on **"Windows PowerShell"** (the blue icon)

#### Step 3: Navigate to the Tool Folder
In PowerShell, type this command and press Enter:
```powershell
cd "$HOME\Desktop\MySnapTest-main\windows"
```

> **📝 Note:** If you extracted the files to a different location, adjust the path accordingly.

#### Step 4: Convert HTML to CSV (Step 1)
Run this command (replace the paths with your actual file locations):
```powershell
.\step1_html_to_csv.ps1 --input "C:\Users\YourName\Desktop\memories_history.html" --output "C:\Users\YourName\Desktop\snap_data.csv"
```

**Example with real paths:**
```powershell
.\step1_html_to_csv.ps1 --input "$HOME\Desktop\memories_history.html" --output "$HOME\Desktop\snap_data.csv"
```

You'll see output like this:
```
╔═══════════════════════════════════════════════════════════════╗
║      HTML to CSV Converter  v2.0                              ║
║      Snapchat Memories Parser (Optimized)                     ║
╚═══════════════════════════════════════════════════════════════╝

Configuration:
  Input:   C:\Users\YourName\Desktop\memories_history.html
  Output:  C:\Users\YourName\Desktop\snap_data.csv
  Size:    2.3 MB

═══════════════════════════════════════════════════════════════
                    CONVERSION COMPLETE
═══════════════════════════════════════════════════════════════

  Rows written:  1234
  Time elapsed:  5s
```

#### Step 5: Download Your Memories (Step 2)
Run this command:
```powershell
.\step2_csv_to_download.ps1 --input "$HOME\Desktop\snap_data.csv" --outdir "$HOME\Desktop\MySnapMemories"
```

This will start downloading all your memories! The process looks like this:
```
+===========================================================================+
|   SSLAXSNAP v3.0 - Snapchat Memories Batch Downloader                    |
+===========================================================================+

BATCH 1/247 - Processing 5 items (1234 remaining)
───────────────────────────────────────────────────────────────────────────
  [OK]    abc12345... HTTP:200 Type:jpeg (1.2s)
          -> 15.12.2023_143022_[lat_51.5_long_-0.12]_0001_image.jpg
  [OK]    def67890... HTTP:200 Type:mp4 (3.5s)
          -> 15.12.2023_143155_[lat__long_]_0002_video.mp4
```

> **⏳ Be patient!** Downloading can take hours if you have thousands of memories.

---

### Mac Instructions

#### Step 1: Download the Tool
1. Click the green **"Code"** button on this page
2. Click **"Download ZIP"**
3. The ZIP will download to your **Downloads** folder
4. Double-click to extract it

#### Step 2: Open Terminal
1. Press **Command (⌘) + Space** to open Spotlight
2. Type **`Terminal`**
3. Press **Enter**

#### Step 3: Navigate to the Tool Folder
```bash
cd ~/Downloads/MySnapTest-main/linux-mac
```

#### Step 4: Make the Scripts Executable
This only needs to be done once:
```bash
chmod +x step1_html_to_csv.sh step2_csv_to_download.sh
```

#### Step 5: Convert HTML to CSV (Step 1)
```bash
./step1_html_to_csv.sh --input ~/Desktop/memories_history.html --output ~/Desktop/snap_data.csv
```

You'll see a nice banner showing your progress:
```
╔═══════════════════════════════════════════════════════════════╗
║      HTML to CSV Converter  v2.0                              ║
║      Snapchat Memories Parser (Optimized)                     ║
╚═══════════════════════════════════════════════════════════════╝

Configuration:
  Input:   /Users/yourname/Desktop/memories_history.html
  Output:  /Users/yourname/Desktop/snap_data.csv

═══════════════════════════════════════════════════════════════
                    CONVERSION COMPLETE
═══════════════════════════════════════════════════════════════

  Rows written:  1234
```

#### Step 6: Download Your Memories (Step 2)
```bash
./step2_csv_to_download.sh --input ~/Desktop/snap_data.csv --outdir ~/Desktop/MySnapMemories
```

---

### Linux Instructions

The steps are the same as Mac! Linux uses the same scripts.

#### Quick Commands
```bash
# Navigate to the linux-mac folder
cd /path/to/MySnapTest/linux-mac

# Make scripts executable (only once)
chmod +x step1_html_to_csv.sh step2_csv_to_download.sh

# Step 1: Convert HTML to CSV
./step1_html_to_csv.sh --input /path/to/memories_history.html --output ./snap_data.csv

# Step 2: Download all memories
./step2_csv_to_download.sh --input ./snap_data.csv --outdir ./MySnapMemories
```

---

## Configuration Options

Both Step 2 scripts have optional settings you can customize:

| Option | Default | Description |
|--------|---------|-------------|
| `--input` | (required) | Path to your CSV file from Step 1 |
| `--outdir` | (required) | Folder where downloads will be saved |
| `--batch` | 5 | How many files to download at the same time |
| `--cooldown` | 3 | Seconds to wait between each batch |
| `--help` | — | Show help message |

### Examples

**Download 10 files at a time (faster, but uses more internet):**
```bash
# Mac/Linux
./step2_csv_to_download.sh --input snap_data.csv --outdir ./MyMemories --batch 10

# Windows
.\step2_csv_to_download.ps1 --input snap_data.csv --outdir .\MyMemories --batch 10
```

**Download slowly with 10-second breaks (gentler on Snapchat's servers):**
```bash
# Mac/Linux
./step2_csv_to_download.sh --input snap_data.csv --outdir ./MyMemories --batch 3 --cooldown 10

# Windows
.\step2_csv_to_download.ps1 --input snap_data.csv --outdir .\MyMemories --batch 3 --cooldown 10
```

---

## Where Are My Files?

Your downloaded memories are organized in folders by date:

```
MySnapMemories/
├── 2023/
│   ├── 01 January/
│   │   ├── 15.01.2023/
│   │   │   ├── 15.01.2023_143022_[lat_51.5_long_-0.12]_0001_image.jpg
│   │   │   ├── 15.01.2023_153045_[lat__long_]_0002_video.mp4
│   │   │   └── ...
│   │   └── 20.01.2023/
│   │       └── ...
│   └── 02 February/
│       └── ...
└── 2024/
    └── ...
```

### Understanding File Names

Each file is named like this:
```
15.01.2023_143022_[lat_51.5_long_-0.12]_0001_image.jpg
│          │       │                    │     │     │
│          │       │                    │     │     └── File type (.jpg, .png, .mp4, .zip)
│          │       │                    │     └── Type label (image/video/filter_archive)
│          │       │                    └── Sequence number
│          │       └── GPS coordinates (if available)
│          └── Time (14:30:22)
└── Date (January 15, 2023)
```

**File Types:**
- `.jpg` / `.png` — Photos
- `.mp4` — Videos  
- `.zip` — Filter archives (Snapchat lens data)

---

## Stopping and Resuming

### Stopping the Download
- **Windows:** Press **Ctrl + C**
- **Mac/Linux:** Press **Ctrl + C**

The script will save your progress before stopping.

### Resuming Downloads
Just run the same Step 2 command again! The script automatically:
- Removes successfully downloaded items from the CSV
- Continues from where you left off
- Skips anything already downloaded

**Example:**
```bash
# This will continue from where you stopped
./step2_csv_to_download.sh --input snap_data.csv --outdir ./MyMemories
```

---

## Troubleshooting

### "File not found" Error
**Problem:** The script can't find your HTML or CSV file.  
**Solution:** Make sure you typed the correct path. Use the full path to be safe:
```powershell
# Windows - use full path
.\step1_html_to_csv.ps1 --input "C:\Users\YourName\Desktop\memories_history.html" --output "C:\Users\YourName\Desktop\snap_data.csv"
```

### "Permission denied" Error (Mac/Linux)
**Problem:** The script isn't allowed to run.  
**Solution:** Make it executable:
```bash
chmod +x step1_html_to_csv.sh step2_csv_to_download.sh
```

### "Script is not digitally signed" Error (Windows)
**Problem:** PowerShell blocks the script for security.  
**Solution:** Run this command first:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```
Type **Y** and press Enter when asked.

### Downloads Are Slow
**Problem:** Files download slowly.  
**Solutions:**
1. Reduce simultaneous downloads: `--batch 3`
2. Check your internet connection
3. Try at a different time (Snapchat servers may be busy)

### Some Files Failed to Download
**Problem:** You see "FAIL" messages during download.  
**Solution:** Don't worry! Just run Step 2 again. Failed items remain in the CSV and will be retried.

### "No rows to process" Message
**Problem:** Step 2 says there's nothing to download.  
**Possible causes:**
1. You already downloaded everything (check your output folder!)
2. The CSV file is empty — run Step 1 again

---

## FAQ

### How long does this take?
It depends on how many memories you have:
- **100 memories:** ~5 minutes
- **1,000 memories:** ~20-30 minutes
- **10,000 memories:** ~3-4 hours
- **40,000 memories:** ~13-14 hours

> **📊 Real-world benchmark:** On a Raspberry Pi 5 with `--batch 10 --cooldown 2`, the tool achieved:
> - **~25 MB/s** download speed
> - **~3,000 files per hour**
> - Very stable with minimal retries

The download speed depends on your internet connection and Snapchat's servers.

### Can I close my computer during download?
No. The download will stop if your computer sleeps or shuts down. But don't worry — just run Step 2 again to continue!

### Is this safe? Will Snapchat ban my account?
This tool downloads YOUR data that Snapchat already gave you. It uses the official download links from your data export. However:
- Use reasonable batch sizes (5-10)
- Include cooldowns between batches
- Don't run multiple downloads at the same time

### What if my download keeps failing?
Try these settings for a more reliable (but slower) download:
```bash
./step2_csv_to_download.sh --input snap_data.csv --outdir ./MyMemories --batch 3 --cooldown 10
```

### Can I run this on multiple computers?
Yes, but not with the same CSV file at the same time. The CSV is updated as downloads complete, so running two copies would cause conflicts.

### The coordinates are empty for most files
That's normal! Snapchat only saves GPS data if you had location services enabled when taking the snap, and many memories don't have location data.

### What are the .zip files?
These are "filter archives" — they contain the lens/filter data for memories that used Snapchat filters. They're usually small and can be ignored or deleted if you don't need them.

---

## Need Help?

If you run into issues not covered here:
1. Check that you're using the correct file paths
2. Make sure your Snapchat data export is complete
3. Try running the commands with `--help` to see all options

---

**Made with ❤️ to help you save your memories forever.**
