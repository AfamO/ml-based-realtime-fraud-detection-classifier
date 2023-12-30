import joblib
from sklearn.ensemble import RandomForestClassifier;
from  sklearn.model_selection import train_test_split;
from sklearn.metrics import accuracy_score;
from sklearn import datasets;
from sklearn.metrics import classification_report,confusion_matrix;
import pandas as pd;
import matplotlib.pyplot as plt;
# Read original dataset
iris_df = pd.read_csv("data/iris.csv")

iris_df = datasets.load_iris();
print(iris_df.target_names);
#print(iris_df);
#iris_df.sample(frac=1, random_state=seed)
# selecting features and target data
# split data into train and test sets
# 70% training and 30% test
X, y = datasets.load_iris(return_X_y=True);
print("Y==",y)

print(plt.hist(y,bins=50))
X_train, X_test, y_train, y_test = train_test_split(
X, y, test_size=0.3, random_state=2023)
print("X-train==",X_train[:,0])
plt.hist(X_train[:,0])
# create an instance of the random forest classifier
clf = RandomForestClassifier(n_estimators=100)
# train the classifier on the training data
clf.fit(X_train, y_train)
# predict on the test set
y_pred = clf.predict(X_test)
# calculate accuracy
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy}") # Accuracy: 0.91
print("Model Train Score==",clf.score(X_train,y_train));
print("Model Test Score==",clf.score(X_test,y_test));
print("Weighted Score::")
print(classification_report(y_test,y_pred));

joblib.dump(clf, "rf_model.sav");