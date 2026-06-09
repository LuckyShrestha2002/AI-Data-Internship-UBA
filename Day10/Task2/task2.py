import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

df = pd.read_csv("../datasets/titanic.csv")

# Create fake datetime column
df["travel_date"] = pd.date_range(
    start="2024-01-01",
    periods=len(df),
    freq="h"
)

# Feature Extraction
df["day_of_week"] = df["travel_date"].dt.dayofweek
df["hour"] = df["travel_date"].dt.hour
df["is_weekend"] = (
    df["day_of_week"] >= 5
).astype(int)

# Interaction Features
df["Fare_Age"] = (
    df["Fare"] *
    df["Age"].fillna(df["Age"].median())
)

df["Family_Size"] = (
    df["SibSp"] +
    df["Parch"]
)

# Log Transform
df["log_fare"] = np.log1p(df["Fare"])

# Binning
df["Age_Group"] = pd.cut(
    df["Age"].fillna(df["Age"].median()),
    bins=[0,18,60,100],
    labels=["Child","Adult","Senior"]
)

# BEFORE
before = pd.read_csv("../datasets/titanic.csv")

before.drop(
    ["PassengerId","Name","Ticket","Cabin"],
    axis=1,
    inplace=True
)

before = pd.get_dummies(before)

before = before.fillna(0)

X_before = before.drop("Survived",axis=1)
y_before = before["Survived"]

X_train,X_test,y_train,y_test = train_test_split(
    X_before,
    y_before,
    test_size=0.2,
    random_state=42
)

model = RandomForestClassifier()
model.fit(X_train,y_train)

pred = model.predict(X_test)

acc_before = accuracy_score(
    y_test,
    pred
)

# AFTER
df.drop(
    ["PassengerId","Name","Ticket","Cabin","travel_date"],
    axis=1,
    inplace=True
)

df = pd.get_dummies(df)

df = df.fillna(0)

X_after = df.drop("Survived",axis=1)
y_after = df["Survived"]

X_train,X_test,y_train,y_test = train_test_split(
    X_after,
    y_after,
    test_size=0.2,
    random_state=42
)

model.fit(X_train,y_train)

pred = model.predict(X_test)

acc_after = accuracy_score(
    y_test,
    pred
)

result = pd.DataFrame({
    "Before":[acc_before],
    "After":[acc_after]
})

result.to_csv(
    "accuracy_comparison.csv",
    index=False
)

print(result)