Cài & chạy ẩn
# Cài git
sudo apt -y update && sudo apt -y install git

# Clone qua HTTPS (không cần SSH key)
sudo git clone https://github.com/<your_user>/tg-pool.git /opt/tg-pool-src
cd /opt/tg-pool-src

# Cài & tạo systemd service
sudo bash install.sh

Sửa cấu hình .env
sudo nano /opt/tg-pool/.env
# Điền: API_ID, API_HASH (bắt buộc)
# Gợi ý đặt API_BEARER (chuỗi bí mật dài)
sudo systemctl restart tg-pool

Upload .session
# Từ Windows PowerShell:
# thay <VPS_IP> và đường dẫn của bạn
scp "C:\Users\<you>\OneDrive\Desktop\acc\*.session" "root@<VPS_IP>:/opt/tg-pool/sessions/"

# Trên VPS:
chmod 600 /opt/tg-pool/sessions/*.session
sudo systemctl restart tg-pool

Kiểm tra trạng thái
systemctl status tg-pool --no-pager
journalctl -u tg-pool -n 100 --no-pager
ss -lntp | grep 8080 || true

# Gọi API (nếu bạn đặt API_BEARER)
curl -H "Authorization: Bearer <API_BEARER>" "http://<VPS_IP>:8080/status"
# Nội bộ VPS:
curl -H "Authorization: Bearer <API_BEARER>" "http://127.0.0.1:8080/status"

API quản lý kênh
# Thêm kênh (auto join, set last_id hiện tại)
curl -H "Authorization: Bearer <API_BEARER>" \
  "http://<VPS_IP>:8080/channel?chanel=<ten_kenh_hoac_@username>"

# Xoá kênh (rời kênh + dừng poll)
curl -H "Authorization: Bearer <API_BEARER>" \
  "http://<VPS_IP>:8080/delete?chanel=<ten_kenh>"

# Xem trạng thái
curl -H "Authorization: Bearer <API_BEARER>" \
  "http://<VPS_IP>:8080/status"

Bật cảnh báo Telegram (tuỳ chọn)
# Điền các biến sau trong /opt/tg-pool/.env rồi restart:
# TELEGRAM_ALERT_BOT_TOKEN=123456:ABC...
# TELEGRAM_ALERT_CHAT_ID=-100xxxxxxxxxx
# TELEGRAM_ALERT_TOPIC_ID=   # (nếu gửi vào Topic)
sudo systemctl restart tg-pool

Test nhanh bot Telegram
curl -s "https://api.telegram.org/bot$TELEGRAM_ALERT_BOT_TOKEN/sendMessage" \
  -H "Content-Type: application/json" \
  -d '{"chat_id": '"$TELEGRAM_ALERT_CHAT_ID"', "text": "Ping test bot OK"}'
# (Nếu gửi vào Topic) thêm: "message_thread_id": <ID_TOPIC>

Giả lập & kiểm tra alert session
# Session chưa login → app sẽ alert “chưa authorized”
touch /opt/tg-pool/sessions/fake_unauthorized.session
journalctl -u tg-pool -f    # xem log ~30s

# Giả lập session đang chạy bị gỡ → app alert “bị gỡ” & reassign
mv /opt/tg-pool/sessions/<file.session> /opt/tg-pool/sessions/_bak.session
# Hoàn tác:
mv /opt/tg-pool/sessions/_bak.session /opt/tg-pool/sessions/<file.session>

Cập nhật code
cd /opt/tg-pool-src
sudo git pull
sudo bash install.sh

Restart / Stop / Log nhanh
sudo systemctl restart tg-pool
sudo systemctl stop tg-pool
journalctl -u tg-pool -f

Mở/giới hạn cổng (nếu cần gọi từ ngoài)
# Mở tạm 8080
sudo ufw allow 8080/tcp

# Giới hạn theo IP
sudo ufw deny 8080/tcp
sudo ufw allow from <YOUR_PUBLIC_IP> to any port 8080 proto tcp
sudo ufw status

SSH tunnel (an toàn, không cần mở port)
# Trên máy bạn:
ssh -L 8080:127.0.0.1:8080 root@<VPS_IP>
# Rồi gọi:
curl -H "Authorization: Bearer <API_BEARER>" http://127.0.0.1:8080/status

Fix lỗi nhanh
# API trả "Invalid token"
grep -E '^API_BEARER=' /opt/tg-pool/.env
sudo systemctl restart tg-pool

# Service không nghe 8080 / app crash
systemctl status tg-pool --no-pager
journalctl -u tg-pool -n 100 --no-pager

# Pydantic "Extra inputs are not permitted"
# → Xoá biến lạ khỏi .env hoặc thêm field tương ứng vào class Cfg trong app.py
