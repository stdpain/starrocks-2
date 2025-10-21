package com.starrocks.mysql;

import java.nio.ByteBuffer;

public class RequestPackage {
    private final int packageId;
    private final ByteBuffer byteBuffer;

    public RequestPackage(int packageId, ByteBuffer byteBuffer) {
        this.packageId = packageId;
        this.byteBuffer = byteBuffer;
    }

    public int getPackageId() {
        return packageId;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }
}
