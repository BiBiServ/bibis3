package de.unibi.cebitec.aws.s3.transfer.util;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import de.unibi.cebitec.aws.s3.transfer.BiBiS3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RegionsProvider {
    private static final Logger log = LoggerFactory.getLogger(S3RegionsProvider.class);

    public static Map<String, Region> getS3Regions() {
        for (int i = 0; i < BiBiS3.RETRIES; i++) {
            try {
                List<Region> s3CapableRegions = RegionUtils.getRegionsForService("s3");
                Map<String, Region> regions = new HashMap<>();
                for (Region s3CapableRegion : s3CapableRegions) {
                    regions.put(s3CapableRegion.getName(), s3CapableRegion);
                }
                return regions;
            } catch (NullPointerException e) {
                log.error("Could not get regions for S3 service. ({} attempts made)", i);
                System.exit(1);
            }
        }
        return null;
    }
}
