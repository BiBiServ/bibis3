
package de.unibi.cebitec.aws.s3.transfer.model.down;


public interface DownloadPart extends IDownloadChunk {
    MultipartDownloadFile getMultipartDownloadFile();

    int getPartNumber();

    void setPartNumber(int partNumber);

    long getInputOffset();

    void setInputOffset(long inputOffset);

    long getOutputOffset();

    void setOutputOffset(long outputOffset);

    long getPartSize();

    void setPartSize(long partSize);
}
