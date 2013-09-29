package com.akwirick.pig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class IPToIntTest {
  private static Cluster cluster;
  private static final Log LOG = LogFactory.getLog(IPToIntTest.class);

  @BeforeClass
  public static void setUpOnce() throws IOException {
    cluster = PigTest.getCluster();
    cluster.update(new Path("src/test/data/pigunit/string_ips.txt"), new Path("string_ips.txt"));
  }

  @Test
  public void testIpConvertToInt() throws ParseException, IOException {

    String [] args = {
        "JAR_PATH=",
        "n=3",
        "reducers=1",
        "input=string_ips.txt",
        "output=integer_ips.txt"
    };

    String [] script = {
        "register $JAR_PATH",
        "define IPToInt com.akwirick.pig.IPToInt()",
        "A = LOAD '$input' AS (string_address:CHARARRAY)",
        "B = FOREACH A GENERATE IPToInt(string_address);",
    };

    PigTest test = new PigTest(script, args);

    String [] input = {
      "139.230.58.169",
      "144.16.91.212",
      "143.245.144.214",
      "129.143.179.215",
      "236.157.212.97",
      "51.171.159.116",
      "179.246.191.78",
      "196.22.150.81",
      "251.208.249.141",
      "224.86.99.112",
      "76.242.71.205",
      "130.250.165.112",
    };

    String [] output = {
      "2347121321",
      "2416991188",
      "2415235286",
      "2173678551",
      "3969766497",
      "866885492",
      "3019292494",
      "3289814609",
      "4224776589",
      "3763757936",
      "1290946509",
      "2214241648"
    };

    test.assertOutput("A", input, "B", output);
  }
}
