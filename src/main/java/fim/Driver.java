package fim;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Driver {

    private static final Logger logger = LogManager.getLogger(Driver.class);
    public static void main(final String[] args) {

        int K = Integer.parseInt(args[2]); //iteration count
        String threshold= args[3]; //min_support

        String input = args[0];//"Test_input_50_10k";
        String output = args[1];//"output/";

        //Calculate L1 job, take the input file, and it will create a K1 folder under the given output
        //directory which will be list of product ids above the threshold.
        String[] hadoopArgs = {input, output  + "/K1", threshold};
        CalculateK1 calculateK1 = new CalculateK1();

        try {
          ToolRunner.run(calculateK1, hadoopArgs);
        } catch (final Exception e) {
          logger.error("", e);
        }
        logger.info("Finished calculate L1");

        //Calculate L2 job, take the input file, and it will create a K1 folder under the given output
        //directory which will be list of product ids above the threshold.
        hadoopArgs = new String[] {input, output + "/K1", output + "/K2", "2", threshold};
        CalculateSupportForCandidates2 calculateSupportForCandidates2 = new CalculateSupportForCandidates2();
        try {
          ToolRunner.run(calculateSupportForCandidates2, hadoopArgs);
        } catch (final Exception e) {
          logger.error("", e);
        }
        logger.info("Finished CalculateSupportForCandidates2");

        //Iterate to calculate support for remaining k values.
        for (int i = 3; i <= K; i++) {

            //Calculate Apriori job takes 4 arguments.
            //1. input (supported candidates for li-1)
            //2. output candidates Ci
            //2. The value of current iteration
            //2. Threshold (min_support)
            hadoopArgs = new String[] {output + "/K" + (i - 1), output + "/C" + i, String.valueOf(i), threshold};
            AprioriGen calculateAprioriGen = new AprioriGen();
            try {
                ToolRunner.run(calculateAprioriGen, hadoopArgs);
            } catch (final Exception e) {
                logger.error("", e);
            }
            logger.info("Finished AprioriGen: "+ i);

            //Calculate support candidates job takes 5 arguments.
            //1. original input (transaction list)
            //2. the candidates Ci
            //3. output directory to store supported candidates (Li)
            //4. The value of current iteration
            //5. Threshold (min_support)
            hadoopArgs = new String[] {input, output + "/C" + i, output + "/K" + i, String.valueOf(i), threshold};
            CalculateSupportForCandidates calculateSupportForCandidates = new CalculateSupportForCandidates();
            try {
                ToolRunner.run(calculateSupportForCandidates, hadoopArgs);
            } catch (final Exception e) {
                logger.error("", e);
            }
            logger.info("Finished calculateSupportForCandidates: "+ i);
        }
    }
}
