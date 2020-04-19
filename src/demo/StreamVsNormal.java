package demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingDouble;

class CustomerProfit {
    final String name;
    final float profit;

    CustomerProfit(String name, float profit) {
        this.name = name;
        this.profit = profit;
    }

    String getName() {
        return name;
    }

    float getProfit() {
        return profit;
    }
}

class Stock {
    final String name;
    final String owner;
    final float purchasePrice;
    final int numberOfShares;

    Stock(String name, String owner, float purchasePrice, int numberOfShares) {
        this.name = name;
        this.owner = owner;
        this.purchasePrice = purchasePrice;
        this.numberOfShares = numberOfShares;
    }
}

class StockService {
    HashMap<String, Float> currentStockPrice;

    StockService() {
        this.currentStockPrice = new HashMap<>();
        this.currentStockPrice.put("microsoft", 175f);
        this.currentStockPrice.put("google", 1300f);
        this.currentStockPrice.put("amazon", 2300f);
        this.currentStockPrice.put("apple", 375f);
        this.currentStockPrice.put("zoho", 250f);
        this.currentStockPrice.put("facebook", 150f);
        this.currentStockPrice.put("sony", 150f);
        this.currentStockPrice.put("nvidia", 275f);
    }

    Map<String, Double> getProfitPerCustomer(String file) throws IOException {
        return Files
                .lines(new File(file).toPath())
                .parallel()
                .filter(this::goodRecords)
                .map(this::parseRecords)
                .map(this::computeProfitPerShare)
                .collect(groupingBy(CustomerProfit::getName, summingDouble(CustomerProfit::getProfit)));
    }

    private CustomerProfit computeProfitPerShare(Stock stock) {
        float profitPerShare = currentStockPrice.get(stock.name) - stock.purchasePrice;
        float totalProfit = stock.numberOfShares * profitPerShare;
        return new CustomerProfit(stock.owner, totalProfit);
    }

    private Stock parseRecords(String line) {
        String[] files = line.split(",");
        return new Stock(files[1], files[0], Float.parseFloat(files[3]), Integer.parseInt(files[2]));
    }

    private boolean goodRecords(String line) {
        return line.split(",").length == 4;
    }
}

class StreamDemo {
    long run(String inputFile) throws IOException {
        long start = System.currentTimeMillis();
        Map<String, Double> profitPerCustomer = new StockService()
                .getProfitPerCustomer(inputFile);
        long timeTaken = System.currentTimeMillis() - start;
        // System.out.println(profitPerCustomer.toString());
        // System.out.println("Stream Time taken: " + timeTaken);

        return timeTaken;
    }
}

class NormalDemo {
    long run(String inputfile) throws FileNotFoundException {
        long start = System.currentTimeMillis();

        Scanner fileScanner = new Scanner(new File(inputfile));

        HashMap<String, Float> result = new HashMap<>();
        HashMap<String, Float> currentStockPrice = new StockService().currentStockPrice;

        while (fileScanner.hasNext()) {
            String line = fileScanner.nextLine();
            String[] splits = line.split(",");
            if (splits.length != 4)
                continue;

            String owner = splits[0];
            String name = splits[1];
            int numberOfShares = Integer.parseInt(splits[2]);
            float purchasePrice = Float.parseFloat(splits[3]);

            float profitPerShare = currentStockPrice.get(name) - purchasePrice;
            float totalProfit = numberOfShares * profitPerShare;

            result.put(owner, result.getOrDefault(owner, 0f) + totalProfit);
        }

        fileScanner.close();

        long timeTaken = System.currentTimeMillis() - start;
        // System.out.println(result.toString());
        // System.out.println("Normal Time taken: " + timeTaken);

        return timeTaken;
    }
}

public class StreamVsNormal {
    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 1000; i++) {
            String inputFile = "src/demo/portfolio.csv";
            long streamTime = new StreamDemo().run(inputFile);
            long normalTime = new NormalDemo().run(inputFile);
            System.out.println(String.format(
                    "Stream time: %d, Normal time: %d, Winner: %s",
                    streamTime, normalTime, (streamTime < normalTime) ? "Stream" : "Normal"));
        }
    }
}