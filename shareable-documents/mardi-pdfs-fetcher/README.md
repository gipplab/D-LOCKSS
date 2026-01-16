# Mardi PDFs - IPFS CID Fetcher

This folder contains a script to fetch PDFs from IPFS using a list of CIDs.

## Usage

```bash
cd shareable-documents/mardi-pdfs
./fetch_cids.sh
```

### Options

You can specify a custom IPFS gateway:

```bash
# Use default gateway (ipfs.io)
./fetch_cids.sh

# Use a different gateway
./fetch_cids.sh https://dweb.link/ipfs/
./fetch_cids.sh https://gateway.pinata.cloud/ipfs/
```

## How it works

1. Reads CIDs from `cid-list.txt` (one per line)
2. Fetches each CID from the IPFS gateway
3. Saves as `{CID}.pdf` in the same folder
4. Skips already downloaded files
5. Verifies downloaded files are PDFs

## Features

- **Resume support**: Skips already downloaded files
- **Error handling**: Continues on failures, reports summary
- **Progress tracking**: Shows progress for each file
- **File verification**: Checks if downloaded files are PDFs
- **Rate limiting**: Small delay between requests to avoid overwhelming gateway

## Output

Files are saved as: `{CID}.pdf`

Example: `bafkreihhali3ddqg2u4mvey2gbgpy56736zakubqlaaitcmodkgujt2vmy.pdf`

## Troubleshooting

If downloads fail:
- Check your internet connection
- Try a different gateway (see options above)
- Re-run the script - it will skip already downloaded files and retry failures
- Some CIDs might be unavailable or the gateway might be rate-limiting

## Alternative Gateways

- `https://ipfs.io/ipfs/` (default)
- `https://dweb.link/ipfs/`
- `https://gateway.pinata.cloud/ipfs/`
- `https://cloudflare-ipfs.com/ipfs/`
