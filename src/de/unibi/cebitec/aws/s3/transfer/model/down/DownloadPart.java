
package de.unibi.cebitec.aws.s3.transfer.model.down;


public interface DownloadPart extends IDownloadChunk{
    
    public MultipartDownloadFile getMultipartDownloadFile();

    public int getPartNumber();

    public void setPartNumber(int partNumber);
    
    public long getInputOffset();

    public void setInputOffset(long inputOffset);

    public long getOutputOffset();

    public void setOutputOffset(long outputOffset);

    public long getPartSize();

    public void setPartSize(long partSize);
    
}
