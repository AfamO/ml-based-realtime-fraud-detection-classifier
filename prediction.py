import joblib


def predict(data):
    model = joblib.load("fraud_detection_model_balanced.sav");
    return model.predict(data);