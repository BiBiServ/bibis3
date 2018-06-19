package de.unibi.cebitec.aws.s3.transfer.model.down.url;

import de.unibi.cebitec.aws.s3.transfer.model.down.IDownloadChunk;

public interface IDownloadChunkUrl extends IDownloadChunk{

    public void download(String url) throws Exception;

}
