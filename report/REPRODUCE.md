# Chạy lại bài trên Windows PowerShell

MSSV: `2A202602682`, trip mô phỏng lỗi: `trip-07774`. Giữ nguyên MSSV có chữ `A` khi chia dataset. Riêng chỉ số chọn trip lỗi lấy phần chữ số: `2202602682 % 26 = 20` trong danh sách trip đã sắp xếp.

Thư mục hiện tại dùng được với bind mount Docker Desktop trên ổ D. `.gitattributes` giữ shell script và file mã nguồn ở LF, tránh lỗi entrypoint CRLF và bảo đảm checksum các file được bảo vệ khớp bản đề.

Tại thư mục gốc, sau khi lấy gói ZIP từ repo đề:

```powershell
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONUTF8 = '1'
'LAB_STUDENT_ID=2A202602682' | Set-Content -Encoding ascii .env
python -m zipfile -e dataset/day09-data.zip .
python scripts/check_data.py
docker compose --profile orchestrate up -d --build --wait --wait-timeout 180
docker compose exec -T -w /opt/lab airflow /home/airflow/lab-venv/bin/python -m pipeline.cli step all --reset
```

Lệnh `docker compose exec` gọi đúng Python và module như `scripts/lab.sh`; dùng được trực tiếp trong PowerShell. Thay phần cuối bằng `step 1 --reset`, `step 2`, `step 3`, `step 4`, `step 5`, hoặc `exp break-gate`, `exp idempotency`, `exp threshold`, `exp leakage`. Unit test:

```powershell
docker compose exec -T -w /opt/lab airflow /home/airflow/lab-venv/bin/python -m unittest discover -s tests
```

`check_submission.py --save-verify` chạy shell script bằng subprocess, không chạy trực tiếp được dưới Python Windows. Trong lượt làm bài này, lưu đầu ra cùng lệnh verify của script bằng Docker trực tiếp, rồi chạy checker gốc:

```powershell
$day09VerifyLines = & docker compose exec -T -w /opt/lab airflow /home/airflow/lab-venv/bin/python -m pipeline.cli verify 2>&1
$day09VerifyExit = $LASTEXITCODE
[System.IO.File]::WriteAllLines(
    (Join-Path (Get-Location) 'report/verify.txt'),
    [string[]]$day09VerifyLines,
    [System.Text.UTF8Encoding]::new($false))
if ($day09VerifyExit -ne 0) { throw 'Verify failed; inspect report/verify.txt' }
python scripts/check_submission.py
```

Với Linux/macOS hoặc môi trường Bash phù hợp, dùng các lệnh nguyên bản trong GUIDE, gồm `python3 scripts/check_submission.py --save-verify`.

Airflow: `http://localhost:8080` (`admin` / `admin`); MinIO: `http://localhost:9001` (`minioadmin` / `minioadmin`). Dashboard:

```powershell
docker compose --profile bi up -d
python scripts/setup_metabase.py
```

Metabase: `http://localhost:3000/dashboard/2` ở máy làm bài, tài khoản lab `lab@example.com` / `Day9-lab-pass!`; ID dashboard có thể khác khi tạo từ đầu. Hai câu SQL riêng nằm trong `report/sql/`, cần thêm lại lên dashboard khi dựng môi trường mới. Dashboard nằm trong volume Postgres, không nằm trong Git; SQL, kết quả và ảnh được giữ trong bài nộp.

Lượt lỗi trên Airflow 2.10.5 đã clear đúng một mapped task đỏ, nhưng bảy downstream vẫn ở `upstream_failed`. Sau khi trip lỗi retry thành công, clear tiếp bảy task chưa chạy này để DAG hoàn tất; không clear các task đã thành công. Chi tiết, request và đối chiếu lần thử trong Câu 9 của REPORT và `report/runs/airflow_*.json`. Bỏ `LAB_FAIL_ONCE` và áp dụng lại Compose trước lượt chạy cuối.
