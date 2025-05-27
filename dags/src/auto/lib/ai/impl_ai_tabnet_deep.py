import numpy as np
import torch
import torch.nn as nn
from pytorch_tabnet.tab_model import TabNetClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.utils.class_weight import compute_class_weight

import src.auto.env as env


def learner(cmp, label_column, df):
    scaler = StandardScaler()

    # X, y ayır
    X = df.drop(columns=[label_column])
    y = df[label_column]

    # Eğitim ve doğrulama verisi
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Sınıf ağırlıklarını hesapla
    class_weights = compute_class_weight(class_weight='balanced', classes=np.unique(y_train), y=y_train)
    class_weights_tensor = torch.tensor(class_weights, dtype=torch.float32).to(env.PRM_GPU_TYPE)

    # Loss fonksiyonu tanımla
    loss_fn = nn.CrossEntropyLoss(weight=class_weights_tensor)

    # TabNet modeli oluştur
    model = TabNetClassifier(
        optimizer_fn=torch.optim.Adam,
        optimizer_params=dict(lr=2e-2),
        scheduler_params={"step_size": 10, "gamma": 0.9},
        scheduler_fn=torch.optim.lr_scheduler.StepLR,
        mask_type='entmax',
        device_name=env.PRM_GPU_TYPE
    )

    # Ölçekleme
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    # Tensöre çevir
    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32, device=env.PRM_GPU_TYPE)
    y_train_tensor = torch.tensor(y_train.values, dtype=torch.int64, device=env.PRM_GPU_TYPE)

    X_val_tensor = torch.tensor(X_val_scaled, dtype=torch.float32, device=env.PRM_GPU_TYPE)
    y_val_tensor = torch.tensor(y_val.values, dtype=torch.int64, device=env.PRM_GPU_TYPE)

    # Modeli eğit
    model.fit(
        X_train=X_train_tensor.cpu().numpy(), y_train=y_train_tensor.cpu().numpy(),
        eval_set=[(X_train_tensor.cpu().numpy(), y_train_tensor.cpu().numpy()),
                  (X_val_tensor.cpu().numpy(), y_val_tensor.cpu().numpy())],
        eval_name=['train', 'valid'],
        max_epochs=200,
        patience=20,
        batch_size=8192,
        virtual_batch_size=512,
        loss_fn=loss_fn
    )

    return model, scaler


def predictor(cmp, class_column, model, scaler, df):
    if df[class_column].dtypes != "object":
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df = df.select_dtypes(numerics)

    X_test = df.drop(columns=[class_column])
    y_test = df[class_column]

    X_test_scaled = scaler.transform(X_test)

    X_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32, device=env.PRM_GPU_TYPE)
    y_test_tensor = torch.tensor(y_test.values, dtype=torch.int64, device=env.PRM_GPU_TYPE)

    y_pred = model.predict(X_test_tensor.cpu().numpy())
    # y_pred_labels = np.argmax(y_pred, axis=1)
    df[f"__#Prediction#__"] = y_pred
    return df
