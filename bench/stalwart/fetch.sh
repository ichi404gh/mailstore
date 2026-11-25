#!/bin/sh
# Download the Stalwart build used in the comparison and check its digest.
# The digest is the one in the release's sigstore bundle (signed by Stalwart's CI on v1.2.0).
set -e
cd "$(dirname "$0")"
version=v1.2.0
asset=stalwart-aarch64-apple-darwin.tar.gz
sha256=7c06bbdff6d3e9ee83d31a2d9ceb21f6dd69173970c57a84870740b326491b26
dir=.bin/$version
mkdir -p "$dir"
gh release download "$version" -R stalwartlabs/stalwart -p "$asset" -D "$dir" --clobber
echo "$sha256  $dir/$asset" | shasum -a 256 -c -
tar xzf "$dir/$asset" -C "$dir"
chmod +x "$dir/stalwart"
"$dir/stalwart" --version
