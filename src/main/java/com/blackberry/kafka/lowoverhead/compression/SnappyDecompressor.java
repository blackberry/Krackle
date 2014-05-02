package com.blackberry.kafka.lowoverhead.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

import com.blackberry.kafka.lowoverhead.Constants;

public class SnappyDecompressor implements Decompressor {
    private byte[] src;
    private int pos;

    private int blockLength;
    private int decompressedLength;

    @Override
    public byte getAttribute() {
	return Constants.SNAPPY;
    }

    @Override
    public int decompress(byte[] src, int srcPos, int length, byte[] dest,
	    int destPos, int maxLength) throws IOException {
	this.src = src;
	decompressedLength = 0;

	// Skip the header -126, 'S', 'N', 'A', 'P', 'P', 'Y', 0
	// version (4 bytes), min compatable version (4 bytes);
	pos = srcPos + 16;

	// TODO: limit the decompressed length
	while (pos < srcPos + length) {
	    blockLength = readInt();
	    decompressedLength += Snappy.uncompress(src, pos, blockLength,
		    dest, destPos + decompressedLength);
	    pos += blockLength;
	}

	return decompressedLength;
    }

    private int readInt() {
	pos += 4;
	return src[pos - 4] << 24 | (src[pos - 3] & 0xFF) << 16
		| (src[pos - 2] & 0xFF) << 8 | (src[pos - 1] & 0xFF);
    }
}
