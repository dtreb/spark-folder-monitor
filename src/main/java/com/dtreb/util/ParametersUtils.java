package com.dtreb.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.regex.Pattern;

/**
 * Useful method to parse program arguments.
 *
 * @author dtreb
 */
public class ParametersUtils {
    // Folder to monitor
    public static final String FOLDER = "monitor";
    // Pattern used to break text into words
    public static final Pattern SPACE = Pattern.compile(" ");
    // Amount of words to show counts for
    public static final int RESULTS_COUNT = 10;
    // Streaming folder check interval
    public static final int CHECK_INTERVAL_SECONDS = 10;

    // Commandline options list
    private static Option FOLDER_OPTION = Option.builder("f")
            .longOpt("folder")
            .required(false)
            .numberOfArgs(1)
            .desc("Local folder path to monitor.")
            .type(String.class)
            .build();
    private static Option COUNT_OPTION = Option.builder("c")
            .longOpt("count")
            .required(false)
            .numberOfArgs(1)
            .type(Number.class)
            .desc("Results count (e.g. top used words count). Default is " + RESULTS_COUNT + ".")
            .build();
    private static Option INTERVAL_OPTION = Option.builder("i")
            .longOpt("interval")
            .required(false)
            .numberOfArgs(1)
            .type(Number.class)
            .desc("Check interval in seconds. Default is " + CHECK_INTERVAL_SECONDS + ".")
            .build();
    private static Option HELP_OPTION = Option.builder("h")
            .longOpt("help")
            .required(false)
            .hasArg(false)
            .build();
    private static Options OPTIONS = new Options()
            .addOption(FOLDER_OPTION)
            .addOption(COUNT_OPTION)
            .addOption(INTERVAL_OPTION)
            .addOption(HELP_OPTION);

    /**
     * Parses command line options according to the specified list ({@link ParametersUtils#OPTIONS}).
     * @param args command line arguments of main method
     * @return parsed {@link CommandLine}
     */
    public static CommandLine parseOptions(String[] args) {
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(OPTIONS, args, true);
            if (commandLine.hasOption(HELP_OPTION.getOpt())) {
                showHelp();
            }
            return commandLine;
        } catch (ParseException ex) {
            showError(ex);
        }
        return null;
    }

    /**
     * Gets name (path) of the local folder to monitor.
     * @param commandLine parsed {@link CommandLine}
     * @return local folder name (path)
     */
    public static String getFolder(CommandLine commandLine) {
        return commandLine.getOptionValue(FOLDER_OPTION.getOpt(), FOLDER);
    }

    /**
     * Gets results count (e.g. top used words count).
     * @param commandLine parsed {@link CommandLine}
     * @return results count
     */
    public static Integer getResultsCount(CommandLine commandLine) {
        try {
            return commandLine.hasOption(COUNT_OPTION.getOpt()) ?
                    ((Number) commandLine.getParsedOptionValue(COUNT_OPTION.getOpt())).intValue()
                    : RESULTS_COUNT;
        } catch (ParseException ex) {
            showError(ex);
        }
        return null;
    }

    /**
     * Gets check interval in seconds.
     * @param commandLine parsed {@link CommandLine}
     * @return check interval in seconds
     */
    public static Integer getIntervalInSeconds(CommandLine commandLine) {
        try {
            return commandLine.hasOption(INTERVAL_OPTION.getOpt()) ?
                    ((Number) commandLine.getParsedOptionValue(INTERVAL_OPTION.getOpt())).intValue()
                    : CHECK_INTERVAL_SECONDS;
        } catch (ParseException ex) {
            showError(ex);
        }
        return null;
    }

    private static void showError(Exception ex) {
        System.out.println("Failed to parse input parameter(s). Error: " + ex.getMessage());
        showHelp();
    }

    private static void showHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar *-jar-with-dependencies.jar <PARAMETERS>", OPTIONS);
        System.exit(0);
    }
}
