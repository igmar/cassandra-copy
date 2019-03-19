package com.palsenberg;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.InetAddress;
import java.net.UnknownHostException;

enum CopyMode {
    DATAONLY,
    TABLETRUNCATE,
    TABLECREATE
}

public class Main {
    public static void main(final String[] args) {

        final Options options = new Options();

        options.addOption("c", false, "Copy data only");
        options.addOption("d", false, "Truncate tables, then copy data");
        options.addOption("dc", false, "Drop tables, recreate, copy data");

        final CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            usage(options);
            System.exit(1);
        }

        CopyMode cm = CopyMode.DATAONLY;
        if (cmd.hasOption("c")) {
            cm = CopyMode.DATAONLY;
        }
        if (cmd.hasOption("d")) {
            cm = CopyMode.TABLETRUNCATE;
        }
        if (cmd.hasOption("dc")) {
            cm = CopyMode.TABLECREATE;
        }

        final String source_keyspace = cmd.getArgs()[1];
        final String destination_keyspace = cmd.getArgs()[3];
        final InetAddress primaryIP;
        final InetAddress secondaryIP;

        DataCopy dc = null;
        try {
            primaryIP = InetAddress.getByName(cmd.getArgs()[0]);
            secondaryIP = InetAddress.getByName(cmd.getArgs()[2]);

            System.out.println(String.format("Migrating keyspace '%s' (%s) to '%s' (%s) with mode %s", source_keyspace, primaryIP.getHostAddress(), destination_keyspace, secondaryIP.getHostAddress(), cm.name()));
            Long start = System.currentTimeMillis();
            dc = new DataCopy(primaryIP, source_keyspace, secondaryIP, destination_keyspace, cm);
            dc.run();
            Long finish = System.currentTimeMillis();

            System.out.print(String.format("Copy took %s seconds", (finish - start) / 1000));
        } catch (RuntimeException | UnknownHostException e) {
            System.err.println(String.format("Exception : '%s'", e.toString()));
            e.printStackTrace();
            System.exit(20);
        } finally {
            if (dc != null)
                dc.close();
        }

        System.exit(0);
    }

    private static void usage(final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("cassandra-copy", options);
    }
}
