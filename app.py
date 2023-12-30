from queue import Queue
from threading import Thread;
import numpy as np;
import pandas as pd;
import streamlit as st;
from kafka import KafkaConsumer;
import atexit
import logging;
import json;

from prediction import predict;

transactionScoreTypes = ["NORMAL","FRAUDULENT"];

global totalTransactionCounter;
global fraudTransactionCounter;
global normalTransactionCounter;
global percentOfFraudTransaction;
def consumePredictAndUpdateTransaction(consumer, q, transactionMetricDict):
    for msg in consumer:
        data = msg.value;
        transactionMetricDict['totalTransactionCounter'] = int(transactionMetricDict['totalTransactionCounter']) + 1;
        print("Consumed Msg==",data);
        transactionDict = data;
            #json.loads(data));
        amount = transactionDict['amount'];
        oldbalanceOrg = transactionDict['oldbalanceOrg'];
        newbalanceOrig = transactionDict['newbalanceOrig'];
        oldbalanceDest = transactionDict['oldbalanceDest'];
        newbalanceDest = transactionDict['newbalanceDest'];
        step =691;
        result = predict(np.array([[step,amount, oldbalanceOrg, newbalanceOrig, oldbalanceDest,newbalanceDest]]));
        index = result[0];
        if index == 0:
            transactionMetricDict['normalTransactionCounter'] = int(transactionMetricDict['normalTransactionCounter']) + 1;
        else:
            transactionMetricDict['fraudTransactionCounter'] = int(transactionMetricDict['fraudTransactionCounter']) + 1;
        transactionMetricDict['percentOfFraudTransaction'] = int((int(transactionMetricDict['fraudTransactionCounter'])/int(transactionMetricDict['totalTransactionCounter'])) * 100);
        predictedFraudStatus = transactionScoreTypes[index];
        print("Prediction::",transactionScoreTypes[index]);
        displayItem = {"fraudStatus": predictedFraudStatus, "data": data,"transactionMetricDict":transactionMetricDict};

        q.put(displayItem);  # Enqueue the display message


transactionConsumer = KafkaConsumer('financial_transaction',
                                    auto_offset_reset='earliest',
                                    bootstrap_servers=['localhost:9092'],
                                    value_deserializer=lambda m:json.loads(m.decode('ascii'))
                                    );
if __name__ == '__main__':
    # Function to be called before exiting
    def exit_app_handler():
        logging.info("Application is ending!");

atexit.register(exit_app_handler);
st.title("Streaming and Detecting Financial Fraud!");

st.markdown("Financial Fraud Detection Model that streams and detects  Fraudulent Transaction in Real-Time ");

fraudDF = pd.read_csv('cleanedBalancedFraudDF.csv');
#fraudDF.drop(['Unnamed: 0 '], axis=1, inplace=True)
#print(fraudDF.head())
#print("My lists pf cols=",fraudDF.columns)
fraudDF = fraudDF[1:5001]
st.header("Current Metrics!");

col1, col2, col3, col4, col5 = st.columns(5,gap="small");
with col1:
    st.caption("Total Number of Transactions");
    stDisplayTotalTransactionsContainer = st.empty();
with col2:
    st.caption("Total Number of Fraudulent Transactions ");
    stDisplayFraudTransactionsContainer = st.empty();

with col3:
    st.caption("Total Number of Normal Transactions ");
    stDisplayNormalTransactionsContainer = st.empty();

with col4:
    st.caption("Percentage of Fraudulent Transactions ");
    stDisplayPercentTransactionsContainer = st.empty();

with col5:
    st.caption("Most Frequent Payment Type Used ");
    st.write(fraudDF['type'].mode().iat[0])
fraudDF = fraudDF.select_dtypes(exclude=['object']);

col1, col2 = st.columns([0.6, 0.4],gap="large");
try:
    with col1:
        st.subheader("New Transaction");
        stDisplayTransactionsContainer = st.empty();
    with col2:
        st.subheader("Transaction Status");
        stDisplayTransactionsStatusContainer = st.empty();
    st.line_chart(fraudDF);


    displayedTransactionDataText ="";
    displayedStatusDataText = "";
    format = "%(asctime)s: %(message)s";
    logging.basicConfig(format=format, level=logging.INFO);
    datefmt = "%H:%M:%S";
    logging.info("Main: before creating Thread");

    queue = Queue();
    transactionMetricDict = {'fraudTransactionCounter':0, 'normalTransactionCounter':0,'totalTransactionCounter' :0 ,'percentOfFraudTransaction':0.0};
    finConsumer = Thread(target=consumePredictAndUpdateTransaction, args=(transactionConsumer, queue, transactionMetricDict));
    logging.info("Main: before running FinConsumer Thread");
    finConsumer.start();

    #Now update the GUI in the main thread
    while True:

        displayItem = queue.get();
        fraudStatus = "{}\n".format(displayItem["fraudStatus"]);
        data = "{}\n".format(displayItem["data"]);
        transactionMetricDict = displayItem["transactionMetricDict"];
        #print("fraudStatus==",fraudStatus,"Trans Data==",data)
        displayedTransactionDataText += data;
        stDisplayTransactionsContainer.text(displayedTransactionDataText);
        displayedStatusDataText += fraudStatus;
        stDisplayTransactionsStatusContainer.text(displayedStatusDataText);
        stDisplayFraudTransactionsContainer.text(transactionMetricDict['fraudTransactionCounter']);
        stDisplayNormalTransactionsContainer.text(transactionMetricDict['normalTransactionCounter']);
        stDisplayTotalTransactionsContainer.text(transactionMetricDict['totalTransactionCounter']);
        stDisplayPercentTransactionsContainer.text(str(transactionMetricDict['percentOfFraudTransaction'])+"%");

except Exception as e:
    print("Fatal Error Occured")
    exit(1)
