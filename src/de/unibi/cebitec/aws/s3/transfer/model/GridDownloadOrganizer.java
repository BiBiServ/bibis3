package de.unibi.cebitec.aws.s3.transfer.model;

import de.unibi.cebitec.aws.s3.transfer.model.down.IDownloadChunk;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GridDownloadOrganizer {

    private static final Logger log = LoggerFactory.getLogger(GridDownloadOrganizer.class);
    private int nodesCount;
    private int currentNode;
    private String feature = "";

    public GridDownloadOrganizer(int nodesCount, int currentNode) {
        this.nodesCount = nodesCount;
        this.currentNode = currentNode;
    }

    public List<IDownloadChunk> getChunkSubset(List<IDownloadChunk> allChunks) {
        int fromIndex = 0;
        if (this.currentNode > 1) {
            fromIndex = (int) Math.ceil(((double) allChunks.size() / (double) this.nodesCount) * ((double) this.currentNode - 1));
        }
        int toIndex = (int) Math.ceil(((double) allChunks.size() / (double) this.nodesCount) * ((double) this.currentNode));
        List<IDownloadChunk> chunkSubset = allChunks.subList(fromIndex, toIndex);
        log.info("Selecting chunk subset from {} to {} of {} ({} chunks). Current node is {}/{}.", fromIndex + 1, toIndex, allChunks.size(), chunkSubset.size(), this.currentNode, this.nodesCount);
        return chunkSubset;
    }

    public int getNodesCount() {
        return nodesCount;
    }

    public String getFeature() {
        return feature;
    }

    public void setFeature(String feature) {
        this.feature = feature;
    }
}
