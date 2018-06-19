package de.unibi.cebitec.aws.s3.transfer.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class StdinInputReader {

    public List<String> getStdinLines() {
        List<String> lines = new ArrayList<>();
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            lines.add(scanner.nextLine());
        }
        return lines;
    }
}
