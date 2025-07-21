# 護理人員排班系統 - Render 雲端部署教學

本教學適用於 Flask + SQLite 專案，並以本系統為例，說明如何將專案部署到 Render 雲端平台。

---

## 1. 專案準備

- 確認專案目錄結構完整（如 `app.py`, `requirements.txt`, `templates/`, `data/` 等）。
- `requirements.txt` 必須包含所有用到的 Python 套件，並建議指定新版本（如下範例）。
- 建議將 SQLite 資料庫放在 `data/` 目錄，並確保程式啟動時自動建立該資料夾：
  ```python
  import os
  os.makedirs('data', exist_ok=True)
  ```

---

## 2. requirements.txt 範例

```
Flask==2.3.3
Werkzeug==2.3.7
pandas>=1.5.0
numpy>=1.23.0
python-dateutil==2.8.2
```

> **注意：** pandas、numpy 請指定較新版本，避免 Render 編譯失敗。

---

## 3. 修改 app.py 啟動方式

Render 會自動設定 `PORT` 環境變數，且必須監聽 `0.0.0.0`。
請將 `app.py` 結尾改為：

```python
import os
port = int(os.environ.get("PORT", 5001))
app.run(debug=False, host='0.0.0.0', port=port)
```

---

## 4. 推送到 GitHub

- 將專案推送到 GitHub 倉庫。
- Render 會自動從 GitHub 拉取程式碼。

---

## 5. Render 平台設置

1. 前往 [https://dashboard.render.com/](https://dashboard.render.com/)
2. 登入後，點選「New +」→「Web Service」
3. 連接你的 GitHub 帳號，選擇專案倉庫
4. 設定如下：
   - **Environment**: Python 3
   - **Build Command**: 
     ```
     pip install --upgrade pip setuptools wheel && pip install -r requirements.txt
     ```
   - **Start Command**: 
     ```
     python app.py
     ```
   - **Root Directory**: 留空或填入專案根目錄
   - **Instance Type**: Free（或依需求選擇）

---

## 6. 常見錯誤與排查

### (1) pandas/numpy 安裝失敗
- 錯誤訊息：`metadata-generation-failed`、`build failed`
- 解法：
  - requirements.txt 指定新版本
  - Build Command 加上 `pip install --upgrade pip setuptools wheel`

### (2) 502/504 Bad Gateway
- 檢查 app.py 是否監聽 `0.0.0.0`，且 port 來自 `os.environ["PORT"]`
- 檢查 requirements.txt 是否缺少必要套件

### (3) SQLite 資料遺失
- Render 的檔案系統是 ephemeral，部署會重置資料。
- 若需長期保存資料，建議改用雲端資料庫（如 PostgreSQL）。

---

## 7. 其他建議

- 若有 SECRET_KEY 等敏感資訊，請在 Render 的「Environment」頁面設定環境變數。
- 靜態檔案（如圖片、CSS）請放在 Flask 預設的 static 目錄。

---

## 8. 完成部署

- Render 會自動部署並給你一個網址（如 `https://your-app.onrender.com`）
- 用瀏覽器測試功能是否正常。

---

如有任何錯誤訊息，請記錄下來並查閱 Render 官方文件或詢問開發社群。 