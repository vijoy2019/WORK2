package com.intuit.cerebro.platform;

public class AppAnnieConfig {

    private final PopulateConfigTable configTable;

    public AppAnnieConfig() {
        configTable = new PopulateConfigTable();
    }

    public static void main(String[] args) {
        AppAnnieConfig appAnnie = new AppAnnieConfig();
        try {
            appAnnie.configAppAnnie();
            appAnnie.configAppAnnieMonthly();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void iOSApp(String product_id, String appName) throws Exception {
        configTable.iOSApp(product_id, appName);
    }

    private void googleApp(String product_id, String appName) throws Exception {
        configTable.googleApp(product_id, appName);
    }

    private void iOSAppMonthly(String product_id, String appName) throws Exception {
        configTable.iOSAppMonthly(product_id, appName);
    }

    private void googleAppMonthly(String product_id, String appName) throws Exception {
        configTable.googleAppMonthly(product_id, appName);
    }


    public void configAppAnnie() throws Exception {
        googleApp("20600005281988", "Track by Wagepoint");
        iOSApp("1072217099", "Track by Wagepoint");

        googleApp("20600000495325", "Xero Accounting and invoices");
        iOSApp("441880705", "Xero Accounting and invoices");

        googleApp("20600004665275", "XeroMe");
        iOSApp("991901494", "XeroMe");

        googleApp("20600004664010", "Robinhood");
        iOSApp("938003185", "Robinhood");

        googleApp("20600003542720", "Acorns");
        iOSApp("883324671", "Acorns");

        googleApp("20600006035056", "Digit");
        iOSApp("1011935076", "Digit");

        googleApp("20600005118024", "Wealthfront");
        iOSApp("816020992", "Wealthfront");

        googleApp("20600005704452", "MoneyLion");
        iOSApp("1064677082", "MoneyLion");

        googleApp("20600001670824", "Betterment");
        iOSApp("393156562", "Betterment");

        googleApp("20600000308824", "Google Pay");
        iOSApp("575923525", "Google Pay");

        googleApp("20600000004594", "PayPal");
        iOSApp("283646709", "PayPal");

        googleApp("20600003300947", "Amazon seller");
        iOSApp("794141485", "Amazon seller");

        googleApp("20600009954574", "WooCommerce");
        iOSApp("888758244", "WooCommerce");

        googleApp("20600000797742", "Creditkarma");
        iOSApp("519817714", "Creditkarma");

        googleApp("20600000370845", "CreditSesame");
        iOSApp("476718980", "CreditSesame");

        googleApp("20600006596942", "Nerdwallet");
        iOSApp("1174471607", "Nerdwallet");

        googleApp("20600005114850", "Clearscore");
        iOSApp("1056640628", "Clearscore");

        iOSApp("853564394", "LendingTree Loan Calculator");
        googleApp("20600002207152", "LendingTree Loan Calculator");

        googleApp("20600005259700", "LendingTree");
        iOSApp("957868548", "LendingTree");

        googleApp("20600008733250", "Noddle");
        iOSApp("1293148355", "Noddle");

        iOSApp("1361797894", "SquareSpace app");
    }

    public void configAppAnnieMonthly() throws Exception {
        googleAppMonthly("20600005281988", "Track by Wagepoint");
        iOSAppMonthly("1072217099", "Track by Wagepoint");

        googleAppMonthly("20600000495325", "Xero Accounting and invoices");
        iOSAppMonthly("441880705", "Xero Accounting and invoices");

        googleAppMonthly("20600004665275", "XeroMe");
        iOSAppMonthly("991901494", "XeroMe");

        googleAppMonthly("20600004664010", "Robinhood");
        iOSAppMonthly("938003185", "Robinhood");

        googleAppMonthly("20600003542720", "Acorns");
        iOSAppMonthly("883324671", "Acorns");

        googleAppMonthly("20600006035056", "Digit");
        iOSAppMonthly("1011935076", "Digit");

        googleAppMonthly("20600005118024", "Wealthfront");
        iOSAppMonthly("816020992", "Wealthfront");

        googleAppMonthly("20600005704452", "MoneyLion");
        iOSAppMonthly("1064677082", "MoneyLion");

        googleAppMonthly("20600001670824", "Betterment");
        iOSAppMonthly("393156562", "Betterment");

        googleAppMonthly("20600000308824", "Google Pay");
        iOSAppMonthly("575923525", "Google Pay");

        googleAppMonthly("20600000004594", "PayPal");
        iOSAppMonthly("283646709", "PayPal");

        googleAppMonthly("20600003300947", "Amazon seller");
        iOSAppMonthly("794141485", "Amazon seller");

        googleAppMonthly("20600009954574", "WooCommerce");
        iOSAppMonthly("888758244", "WooCommerce");

        googleAppMonthly("20600000797742", "Creditkarma");
        iOSAppMonthly("519817714", "Creditkarma");

        googleAppMonthly("20600000370845", "CreditSesame");
        iOSAppMonthly("476718980", "CreditSesame");

        googleAppMonthly("20600006596942", "Nerdwallet");
        iOSAppMonthly("1174471607", "Nerdwallet");

        googleAppMonthly("20600005114850", "Clearscore");
        iOSAppMonthly("1056640628", "Clearscore");

        iOSAppMonthly("853564394", "LendingTree Loan Calculator");
        googleAppMonthly("20600002207152", "LendingTree Loan Calculator");

        googleAppMonthly("20600005259700", "LendingTree");
        iOSAppMonthly("957868548", "LendingTree");

        googleAppMonthly("20600008733250", "Noddle");
        iOSAppMonthly("1293148355", "Noddle");

        iOSAppMonthly("1361797894", "SquareSpace app");
    }

}
