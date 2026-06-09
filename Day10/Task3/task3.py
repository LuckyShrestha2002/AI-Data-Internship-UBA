import pandas as pd
import time

from sklearn.preprocessing import LabelEncoder, OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

# Load dataset
df = pd.read_csv("../datasets/titanic.csv")

# Drop unnecessary columns
df.drop(
    ["PassengerId", "Name", "Ticket", "Cabin"],
    axis=1,
    inplace=True
)

# Fill missing values safely
df = df.fillna(0)

cat_cols = ["Sex", "Embarked"]

results = []


# LABEL ENCODING

label_df = df.copy()

for col in cat_cols:
    le = LabelEncoder()

    # FIX: ensure uniform type (IMPORTANT)
    label_df[col] = label_df[col].astype(str)

    label_df[col] = le.fit_transform(label_df[col])

X = label_df.drop("Survived", axis=1)
y = label_df["Survived"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

start = time.time()

model = LogisticRegression(max_iter=1000)
model.fit(X_train, y_train)

end = time.time()

pred = model.predict(X_test)

results.append([
    "Label",
    accuracy_score(y_test, pred),
    end - start,
    X.shape[1]
])


# ONE HOT ENCODING

ohe_df = pd.get_dummies(df)

X = ohe_df.drop("Survived", axis=1)
y = ohe_df["Survived"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

start = time.time()

model.fit(X_train, y_train)

end = time.time()

pred = model.predict(X_test)

results.append([
    "OneHot",
    accuracy_score(y_test, pred),
    end - start,
    X.shape[1]
])


# ORDINAL ENCODING

ord_df = df.copy()

oe = OrdinalEncoder()

# FIX: ensure no mixed types
ord_df[cat_cols] = ord_df[cat_cols].astype(str)

ord_df[cat_cols] = oe.fit_transform(ord_df[cat_cols])

X = ord_df.drop("Survived", axis=1)
y = ord_df["Survived"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

start = time.time()

model.fit(X_train, y_train)

end = time.time()

pred = model.predict(X_test)

results.append([
    "Ordinal",
    accuracy_score(y_test, pred),
    end - start,
    X.shape[1]
])


# RESULTS TABLE

results_df = pd.DataFrame(
    results,
    columns=["Encoding", "Accuracy", "Training_Time", "Feature_Count"]
)

results_df.to_csv("encoding_results.csv", index=False)

print(results_df)