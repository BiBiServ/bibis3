package de.unibi.cebitec.aws.s3.transfer.util;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3RegionsProvider {

    public static Map<String, Region> getS3Regions() {
        List<Region> s3CapableRegions = RegionUtils.getRegionsForService("s3");
        Map<String, Region> regions = new HashMap<>();
        for (Region s3CapableRegion : s3CapableRegions) {
            regions.put(s3CapableRegion.getName(), s3CapableRegion);
        }
        return regions;
    }
}
