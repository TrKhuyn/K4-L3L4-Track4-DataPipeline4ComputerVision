# REPORT — Lab Day 9

> Kết quả chạy thực tế trên Docker Desktop, Windows, ngày **12/10/2026**. Lượt cuối hoàn tất lúc **20:31 (UTC+7)**. Số liệu được đối chiếu với log trong `report/runs/` và JSON trong `report/evidence/`; cách chạy trực tiếp từ PowerShell ở [REPRODUCE.md](report/REPRODUCE.md).

| | |
|---|---|
| Họ và tên | Trần Ngọc Khuyến |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602682` |
| GitHub | TrKhuyn |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07774` |

MSSV có chữ `A`, nên lệnh `int(MSSV)` trong GUIDE không áp dụng trực tiếp. Riêng việc chọn trip lỗi dùng phần chữ số `2202602682 % 26 = 20`, lấy phần tử thứ 20 (chỉ số từ 0) trong danh sách trip đã sắp xếp. `LAB_STUDENT_ID` vẫn giữ nguyên `2A202602682` khi chia train/val/test.

## Kết quả cuối

- `dataset_version` của bạn: `00c5f985d775` (đối chiếu `report/evidence/run_summary.json`).
- Số khung hình train / val / test của bạn: **781 / 129 / 109**, tổng **1019**; số trip **18 / 4 / 4**, tổng **26**.
- [report/verify.txt](report/verify.txt): đầu ra verify thực tế **27/27 checks passed**, lưu bằng lệnh Docker tương đương `scripts/lab.sh verify` trên PowerShell.
- [Graph baseline](report/screenshots/airflow_graph.png), [MinIO lake](report/screenshots/minio_lake.png), [Metabase dashboard](report/screenshots/metabase_dashboard.png), [log task đỏ](report/screenshots/fail_task_log.png), [Graph sau phục hồi](report/screenshots/airflow_retry_graph.png).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Quan sát [log bước 1](report/runs/step1.txt): A có `ts_utc` dạng ISO UTC (`2025-10-07T08:00:00.014Z`), B có `timestamp_ms` dạng epoch ms (`1759933801509`). A dùng `speed_mps` theo m/s, B dùng `spd_kmh` theo km/h; `gas`/`brake` của A là tỷ lệ 0–1, còn `throttle_pct`/`brake_pct` của B là phần trăm. Các cột còn đổi tên: `heading_deg` → `hdg`, `lat`/`lon` → `latitude`/`longitude`, `acc_x`/`acc_y` → `ax`/`ay`, `steer` → `steering`. Đồng hồ B chạy trước video 1500 ms, A không lệch.

Bronze giữ hai schema nguồn riêng tại `bronze.raw_telemetry_a` và `bronze.raw_telemetry_b`; các cột CSV giữ dạng chữ, kể cả ô rỗng, giúp giữ nguyên giá trị lỗi để truy nguyên và xử lý lại bằng quy tắc Silver. Video thô ở `bronze/video/<drive>/raw.mp4`, metadata trong `bronze.video_files`, trip trong `bronze.trips_raw`, bảng Delta ở `bronze/delta/`.

Lượt nạp lặp A báo `already present`; số dòng vẫn là A **3021**, B **3234**, video **2**, trip **26**. [Lịch sử Delta A](report/runs/bronze_history.txt) chỉ có **một commit**, version **0**, thêm 3021 dòng. File được nhận diện bằng SHA256, nên chạy lại không nhân đôi dữ liệu; module 1 đạt **5/5**.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_a` chuyển ISO UTC thành `timestamptz`, giữ tốc độ m/s; `stg_telemetry_b` đổi tên cột, chia km/h cho **3,6**, chia bàn đạp phần trăm cho **100**, lấy `(timestamp_ms - 1500) / 1000` rồi `to_timestamp`. Tốc độ rỗng được chuyển thành NULL bằng `nullif` và `try_cast`, sau đó phân loại trước khi đưa vào bảng sạch.

[Bước 2](report/runs/step2.txt) cho A **2968** dòng sạch, B **3181**; thời điểm đầu B là **2025-10-08 14:30:00.009 UTC**, chỉ sau video **9 ms**, thay vì 1509 ms. Tốc độ lớn nhất sau chuẩn hóa lần lượt khoảng **71** và **100 km/h**; module 2 đạt **7/7**.

| Drive | duplicate | speed_missing | speed_out_of_range | Tổng rejects | Sạch + rejects = thô |
|---|---:|---:|---:|---:|---:|
| drive_a | 20 | 30 | 3 | 53 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 + 53 = 3234 |

`silver.silver_telemetry_rejects` giữ timestamp, lý do, `_batch_id` và `_row_no` ([mẫu rejects](report/runs/rejects_sample.txt)); bản Delta ở `silver/delta/telemetry_rejects/`. Cách ly giữ dấu vết để tìm lỗi cảm biến hoặc sửa luật và xử lý lại, đồng thời đối soát đủ **6255** dòng đầu vào = **6149** sạch + **106** rejects.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` cảnh báo khi khoảng cách giữa hai mẫu cùng drive vượt **1000 ms**: mất tín hiệu là lỗi đã biết, cần điều tra nhưng vẫn có thể dùng các khoảng còn tốt. `in_range` mặc định là `error`, yêu cầu tốc độ **0–70 m/s**; dữ liệu vượt miền hợp lệ sẽ làm sai nhãn huấn luyện nên phải chặn bước sau.

[break_gate.json](report/evidence/break_gate.json) ghi việc nới luật làm sạch lên **1000 m/s**: test **`in_range_silver_telemetry_speed_mps__70__0`** bắt **6 dòng**, kết quả **PASS=5 WARN=1 ERROR=1 SKIP=0**. Sau khi script khôi phục luật gốc, kết quả là **PASS=6 WARN=1 ERROR=0 SKIP=0**. Cổng kiểm tra độc lập đã bắt được lỗi dù bước biến đổi vẫn chạy thành công; trong DAG, task test lỗi ngăn Gold và các bước xuất dữ liệu chạy tiếp.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

[idempotency.json](report/evidence/idempotency.json) thử **`trip-00003`**, có **60 frame**. Toàn bảng trước và sau hai lần chạy lại đều **1246 dòng**, dấu vân tay đều **`6c7480dfde7e`** (`unchanged=true`). Bảng thử dùng append hai lần có **120 dòng**, tức gấp **2** số frame thực của trip. [Unit test](report/runs/unit_tests.txt) đạt **9/9**, module 3 đạt **6/6**.

Pipeline ghi Delta `overwrite` với predicate `trip_id = ...`, thay tập dòng của một trip trong một commit thay vì xóa các dòng cũ rồi ghi mới bằng hai thao tác rời. Nếu ghi thất bại trước commit, người đọc vẫn thấy snapshot cũ; tránh khoảng trống hoặc tập dòng dở dang do xóa trước. Bảng được partition vật lý theo `drive_id`, còn phạm vi thay thế logic là một trip. File JPG được ghi trước commit Delta nên vẫn có thể có file chưa được bảng tham chiếu nếu lỗi; tính nguyên tử này áp dụng cho bảng, không cho mọi object S3 cùng lúc.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

[threshold.json](report/evidence/threshold.json) cho thấy ngưỡng tăng từ 0 lên 6 làm số frame trùng tăng mạnh; tôi giữ **2 bit** để bắt khung gần như không đổi nhưng tránh loại quá nhiều thay đổi nhỏ có ích. dHash là tín hiệu hình ảnh, không chứng minh được camera đứng hình: khi xe chậm hoặc đứng yên, cảnh hợp lệ cũng có thể gần trùng; khi xe nhanh, cảnh đổi nhiều, ngưỡng lớn có thể loại nhầm frame khác nhau. Nên kiểm tra ảnh và chuyển động, không dùng một ngưỡng chung như kết luận tuyệt đối.

Khung **đen** có brightness **<12**, bị loại ở Gold; khung **tối** có brightness **từ 12 đến dưới 40**, chỉ được gắn `is_dark`. Tôi đồng ý giữ điều kiện thiếu sáng thực tế để dataset không thiên lệch về ban ngày, nhưng vẫn loại nếu frame đồng thời mờ, trùng hoặc mất đồng bộ. [Bước 3](report/runs/step3.txt): A có **602** frame, **6** đen, **9** mờ, **4** tối, **149** gần trùng, **7** mất đồng bộ; B có **644**, tương ứng **4**, **0**, **61**, **53**, **7**. Các cờ chồng lấn nên không cộng chúng để tính số bị loại. Gap ASOF trung bình A/B **66/73 ms**, lớn nhất **2884/3112 ms**: telemetry dùng là mẫu mới nhất trước hoặc đúng event time, không phải mẫu gần nhất ở cả hai phía.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

[leakage.json](report/evidence/leakage.json) thử **1019 frame** từ **26 trip**: chia bằng hash từng frame làm **26/26 trip** nằm ở nhiều split, và cả **26** có frame trong train lẫn test. Gold chia theo trip có **0 trip** bị chia cắt. Các ảnh liền nhau cách khoảng 0,5 giây thường cùng cảnh, vật thể và góc camera; mô hình đã thấy cảnh gần giống trong train có thể đạt điểm test cao mà chưa tổng quát hóa sang chuyến khác. Chia theo trip giảm rò rỉ này; vẫn cần kiểm tra các trip khác nhau có đi qua cùng cảnh hay không.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

[Bước 4](report/runs/step4.txt) và [export lần hai](report/runs/export_repeat.txt) đều cho **`00c5f985d775`**, **1019 frame**, split **781/129/109**; không trip nào ở hai split, module 4 đạt **7/7**. SHA256 nội dung cho phép nhận diện cùng tập frame và cách chia khi chạy lại, thay vì tạo tên mới chỉ vì thời gian chạy đổi.

Cụ thể, `publish.export_dataset` sắp xếp theo `frame_id`, băm các dòng `frame_id|split`, lấy **12 ký tự hex** đầu; version này không băm toàn bộ byte ảnh, telemetry hoặc lineage. MSSV `2A202602682` được nối với `trip_id` trong phép md5 để xếp trip trong từng drive, nên MSSV khác thường tạo thứ tự, split và version khác; không phải cam kết mọi MSSV luôn khác version. A có train/val/test **334/57/45**, B **447/72/64**, mỗi drive có **9/2/2 trip**. Lineage và manifest cần được lưu cùng version để biết chính xác nguồn và quy tắc tạo dataset.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest thật ở **`s3://lake/gold/datasets/00c5f985d775/manifest.json`**, có bản lưu [manifest.json](report/evidence/manifest.json) từ lượt cuối. Lineage ghi:

| Bảng nguồn | Delta version |
|---|---:|
| bronze.raw_telemetry_a | 0 |
| bronze.raw_telemetry_b | 0 |
| bronze.trips_raw | 0 |
| bronze.video_files | 1 |
| silver.frames | 25 |
| silver.telemetry | 0 |
| silver.frames_flagged | 0 |
| silver.frame_telemetry | 0 |

`input_video_sha256` nhận diện video A **`636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`**, B **`bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`**. Dataset dùng ảnh trích từ hai video này, metadata trip và telemetry chuẩn hóa/đã chỉnh clock offset, ghép event time; quy tắc drop là `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`, split theo trip và phân tầng theo drive. `manifest_sha256` của manifest Parquet là **`f6c6075f33ebf2002c3425716424b74919f9018644a4c0f4ef826d716de686ed`**.

Để tái tạo sau 3 tháng cần giữ file nguồn khớp [data/MANIFEST.json](data/MANIFEST.json), các snapshot Delta hoặc dữ liệu đủ để dựng lại, commit mã nguồn, môi trường thư viện, ngưỡng dbt, lấy mẫu **2 fps**, clock offset B **1500 ms** và MSSV **`2A202602682`**. Version bảng chỉ có ý nghĩa trong lịch sử của cùng lake; reset xóa lịch sử, nên manifest đơn lẻ không thay thế bản lưu dữ liệu. Version `silver.frames=25` là 26 lần ghi trip, đếm từ 0; lượt thí nghiệm trước đó có thêm overwrite nhưng nội dung frame/split và dataset_version vẫn giống nhau.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Đặt `LAB_FAIL_ONCE=trip-07774`, chạy `day09_reset` rồi `day09_video_pipeline`. Lượt baseline đã xanh **37/37 task instance** (**26** `silver_frames` và **11** task khác). Lượt mô phỏng có **1 task đỏ**: `silver_frames`, **map_index=20**, log ghi **`simulated crash while extracting trip-07774; run it again`**; **7** downstream `upstream_failed`, **29** task thành công. [Ảnh log](report/screenshots/fail_task_log.png) và [Graph lỗi](report/screenshots/airflow_failed_graph.png) ghi đúng trip và trạng thái.

Đầu tiên chỉ clear task đỏ: API preview xác nhận đúng **1** task được clear, giữ `include_upstream=false`, `include_downstream=false`. Trip chạy lại thành công, nhưng Airflow **2.10.5** vẫn giữ **7** task `upstream_failed`, DAG lại failed với **30 success / 7 upstream_failed**; khác với kỳ vọng “chỉ clear đỏ là xanh” của GUIDE. Sau đó clear trạng thái của đúng **7 task downstream chưa từng chạy** (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) để chúng thực thi lần đầu, không reset lake hoặc chạy lại Bronze/25 trip tốt.

Cuối cùng DAG xanh **37/37** ([Graph sau xử lý](report/screenshots/airflow_retry_graph.png)). [airflow_recovery.json](report/runs/airflow_recovery.json) đối chiếu start/end time và try number: **29 task thành công ban đầu không chạy lại**, task lỗi đạt **try_number=2**. Dữ liệu sau phục hồi vẫn **1246 frame Silver**, **1019 frame Gold**, `dataset_version` **`00c5f985d775`**, giống baseline; [verify sau phục hồi](report/runs/recovery_verify.txt) đạt **27/27**. Đã bỏ `LAB_FAIL_ONCE` trước lượt chạy cuối; các request clear và snapshot trạng thái được lưu trong `report/runs/airflow_*.json`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Đây là dự đoán từ kiến trúc lab, chưa phải kết quả benchmark 500 xe; thứ tự nghẽn thực tế còn phụ thuộc thời lượng video và tải truy vấn. Liên hệ các chủ đề kiến trúc phân tán, ELT và xử lý ở quy mô lớn của bài học:

| Thành phần | Giới hạn quan sát từ repo | Hướng thay đổi |
|---|---|---|
| Worker trích frame / Airflow LocalExecutor | Một container, parallelism 2 và chỉ 1 `silver_frames` chạy trong mỗi DAG run; mỗi trip lại tải toàn bộ video | Chạy FFmpeg theo trip trên nhiều worker, cache/video range phù hợp; dùng [CeleryExecutor và hàng đợi](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html), vẫn giữ retry theo trip và giới hạn ghi Delta để tránh xung đột |
| dbt + DuckDB trên một máy | Transform và join cùng phụ thuộc file `work/lakehouse.duckdb`, CPU/RAM của một container | Chuyển xử lý bảng lớn sang [Spark với các executor trên cluster](https://spark.apache.org/docs/latest/cluster-overview.html); tính toán theo ngày/xe/trip và incremental, giữ quality gate trước Gold |
| MinIO một node / một volume | Video, ảnh JPG và Delta đều dùng ổ của một máy, nhiều object nhỏ tạo áp lực I/O; lỗi node ảnh hưởng cả lake | Dùng object storage phân tán, ví dụ [MinIO nhiều node/nhiều ổ](https://min.io/docs/minio/linux/operations/install-deploy-manage/deploy-minio-multi-node-multi-drive.html); compact file Delta, quản lý vòng đời ảnh và chỉ đưa mart cần thiết sang warehouse |

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu hỏi: **drive nào có tỷ lệ frame được giữ thấp hơn, để ưu tiên điều tra chất lượng đầu vào?** Đã thêm card “Khuyến: tỷ lệ frame giữ lại theo drive” trên [dashboard cục bộ](http://localhost:3000/dashboard/2).

```sql
SELECT drive_id,
       SUM(frames) AS frames_total,
       SUM(frames_kept) AS frames_kept,
       SUM(frames) - SUM(frames_kept) AS frames_dropped,
       ROUND(100.0 * SUM(frames_kept) / NULLIF(SUM(frames), 0), 2) AS kept_pct
FROM marts.mart_trip_summary
GROUP BY drive_id
ORDER BY drive_id;
```

| Drive | Tổng | Giữ | Loại | Tỷ lệ giữ |
|---|---:|---:|---:|---:|
| drive_a | 602 | 436 | 166 | 72,43% |
| drive_b | 644 | 583 | 61 | 90,53% |

A cần được ưu tiên điều tra: tỷ lệ giữ thấp hơn B **18,10 điểm phần trăm**. Đây là số frame bị loại duy nhất tính bằng tổng trừ giữ, tránh cộng các cờ bị chồng lấn. Kết quả truy vấn và ID card lưu tại [metabase_questions.json](report/runs/metabase_questions.json), SQL tại [retention_by_drive.sql](report/sql/retention_by_drive.sql).

### Bonus: câu hỏi vận hành thứ hai

**Năm trip nào mất tỷ lệ frame lớn nhất để kiểm tra log, ảnh và chất lượng cảm biến trước?** Card “Khuyến: 5 trip cần điều tra chất lượng” dùng truy vấn:

```sql
SELECT drive_id, trip_id, lighting, conditions, avg_speed_kmh,
       frames, frames_kept, frames - frames_kept AS frames_dropped,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS dropped_pct
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC, trip_id
LIMIT 5;
```

| Trip (đều drive_a) | Loại / tổng | Tỷ lệ loại | Tốc độ TB (km/h) |
|---|---:|---:|---:|
| trip-04622 | 34 / 60 | 56,67% | 67,1 |
| trip-03904 | 11 / 22 | 50,00% | 46,1 |
| trip-02086 | 28 / 60 | 46,67% | 17,6 |
| trip-08888 | 24 / 60 | 40,00% | 6,5 |
| trip-07774 | 13 / 34 | 38,24% | 12,8 |

Ưu tiên `trip-04622`: mất hơn một nửa frame dù tốc độ trung bình cao; không thể quy mọi trường hợp trùng cho xe chạy chậm. Cần mở frame và cờ chất lượng để xác định nguyên nhân, vì query mart này chỉ xếp mức độ mất dữ liệu, không chứng minh lỗi camera. SQL lưu tại [trips_to_investigate.sql](report/sql/trips_to_investigate.sql); bonus không thay đổi cấu hình pipeline chính.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Quan sát đáng chú ý: B có **61 frame tối** nhưng giữ thực sự **58** trong Gold; “tối” không đồng nghĩa với “xấu”, còn 3 frame bị loại bởi các cờ khác.
A có 4 frame tối và giữ đủ 4 ([truy vấn](report/runs/dark_frames.txt)).
Chưa có cơ sở chọn ngưỡng dHash tối ưu nếu chỉ nhìn số frame; cần kiểm tra ảnh, vận tốc và tác động lên mô hình.
Các quality gate hiện chưa kiểm tra đầy đủ miền GPS, gia tốc, hướng và bàn đạp; tốc độ hợp lệ vẫn có thể đi kèm tọa độ sai.
Chia theo trip chưa loại hết rò rỉ nếu hai trip khác nhau đi qua cùng cảnh; nên kiểm tra thêm theo tuyến đường và thời gian ghi nhận.
