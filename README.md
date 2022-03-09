# 訊息通知系統

### 說明
接手時，系統推播功能失效，無法將警訊推送給使用者，且不知道原因；

### 達成效果
確認程式，並搭配rabbitMQ rabbitmq_management確認系統運行，發現queue卡住的原因是mongo資料庫存入錯誤，導致撈取出來的資料進入queue後，無法被接收，而卡住。
修改存入mongo的資料格式後，訊息通知系統可正常運作。

### 使用技術
rabbitMQ, Docker
