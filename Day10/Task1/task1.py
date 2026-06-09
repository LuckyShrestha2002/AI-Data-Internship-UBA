import pandas as pd
import numpy as np

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

# Load data
df = pd.read_csv("../datasets/titanic.csv")

print("Original Shape:", df.shape)

# Drop low value columns
df.drop(
    ["PassengerId", "Name", "Ticket", "Cabin"],
    axis=1,
    inplace=True
)

# Target
y = df["Survived"]

# Features
X = df.drop("Survived", axis=1)

# Numeric and categorical columns
num_cols = X.select_dtypes(include=np.number).columns
cat_cols = X.select_dtypes(include="object").columns

# Median imputation for numeric
num_imputer = SimpleImputer(strategy="median")
X[num_cols] = num_imputer.fit_transform(X[num_cols])

# Mode imputation for categorical
cat_imputer = SimpleImputer(strategy="most_frequent")
X[cat_cols] = cat_imputer.fit_transform(X[cat_cols])

# One Hot Encoding
X = pd.get_dummies(
    X,
    columns=cat_cols,
    drop_first=True
)

# Train Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42
)

# Scaling
scaler = StandardScaler()

X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Save outputs
pd.DataFrame(X_train_scaled).to_csv(
    "X_train.csv",
    index=False
)

pd.DataFrame(X_test_scaled).to_csv(
    "X_test.csv",
    index=False
)

pd.DataFrame(y_train).to_csv(
    "y_train.csv",
    index=False
)

pd.DataFrame(y_test).to_csv(
    "y_test.csv",
    index=False
)

print("Task 1 Completed")