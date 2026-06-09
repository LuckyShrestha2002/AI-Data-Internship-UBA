import pandas as pd
import matplotlib.pyplot as plt

from sklearn.datasets import load_wine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import (
    StandardScaler,
    MinMaxScaler,
    RobustScaler
)

from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score

wine = load_wine()

X = pd.DataFrame(
    wine.data,
    columns=wine.feature_names
)

y = wine.target

X_train,X_test,y_train,y_test = train_test_split(
    X,y,test_size=0.2,random_state=42
)

scalers = {
    "Raw":None,
    "Standard":StandardScaler(),
    "MinMax":MinMaxScaler(),
    "Robust":RobustScaler()
}

results=[]

for scaler_name, scaler in scalers.items():

    if scaler:
        Xtr=scaler.fit_transform(X_train)
        Xte=scaler.transform(X_test)
    else:
        Xtr=X_train
        Xte=X_test

    knn=KNeighborsClassifier(n_neighbors=5)
    knn.fit(Xtr,y_train)

    pred=knn.predict(Xte)

    results.append([
        scaler_name,
        "KNN",
        accuracy_score(y_test,pred)
    ])

    svm=SVC()

    svm.fit(Xtr,y_train)

    pred=svm.predict(Xte)

    results.append([
        scaler_name,
        "SVM",
        accuracy_score(y_test,pred)
    ])

results_df = pd.DataFrame(
    results,
    columns=[
        "Scaler",
        "Model",
        "Accuracy"
    ]
)

results_df.to_csv(
    "scaler_results.csv",
    index=False
)

pivot = results_df.pivot(
    index="Scaler",
    columns="Model",
    values="Accuracy"
)

pivot.plot(kind="bar")

plt.title("Scaler Comparison")

plt.tight_layout()

plt.savefig(
    "scaler_comparison.png"
)

plt.show()

print(results_df)