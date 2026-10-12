# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Đoàn Duy Bách |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602515` |
| GitHub | DuyBach2003 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-03904` (MSSV có chữ `A` nên lấy các chữ số `2202602515 % 26 = 9` → phần tử thứ 9 của `list-trips`) |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Năm khác biệt thấy trực tiếp ở hai bảng bronze (`raw_telemetry_a` 3021 dòng, `raw_telemetry_b` 3234 dòng):

| | Nguồn A | Nguồn B |
|---|---|---|
| Tên cột | `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer` | `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering` |
| Timestamp | chuỗi ISO UTC `2025-10-07T08:00:00.014Z` | epoch mili-giây `1759933801509` |
| Tốc độ | m/s (`1.213`) | km/h (`55.11`) |
| Bàn đạp ga/phanh | tỉ lệ 0–1 (`gas=0.028`) | phần trăm (`throttle_pct=2.0`) |
| Đồng hồ | khớp video (`telemetry_clock_offset_ms = 0`) | chạy nhanh hơn video **1500 ms** (`bronze_video_files`): mẫu đầu `1759933801509` = 14:30:01.509 trong khi video bắt đầu 14:30:00 |

Giữ mỗi nguồn một bảng vì hai schema khác nhau: gộp ngay buộc phải chọn tên cột, đơn vị và cách sửa đồng hồ ngay lúc nạp, và nếu chọn sai thì bản gốc đã mất. Mọi cột ở dạng `VARCHAR` (kiểm tra bằng `information_schema`, chỉ `_row_no` và `_ingested_at` là cột kỹ thuật có kiểu) để bronze **không bao giờ từ chối hay làm méo dữ liệu**: ô trống, `speed` vô lý hay timestamp lệch vẫn được ghi đúng như nguồn gửi, có `_source_file`, `_batch_id`, `_row_no` để truy về dòng CSV. Ép kiểu và luật nghiệp vụ để ở silver (dbt), nên khi luật đổi ta chỉ cần chạy lại silver từ bronze, không phải nạp lại từ nguồn. Bronze cũng idempotent: lần nạp thứ hai báo `already present` vì file được nhận diện bằng sha256 (`636ba1a9…` cho A, `bfa62703…` cho B), và `cli history bronze.raw_telemetry_a` chỉ có **một commit** (version 0, `rows_added: 3021`).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_a.sql` và `stg_telemetry_b.sql` đưa cả hai về cùng một bộ cột (`ts, speed_mps, heading_deg, lat, lon, …, gas, brake, steer`):
- **Đơn vị**: B có `spd_kmh / 3.6` → m/s, `throttle_pct / 100` và `brake_pct / 100` → tỉ lệ 0–1 như A.
- **Kiểu thời gian**: A `cast(ts_utc as timestamptz)`; B `to_timestamp(timestamp_ms / 1000.0)` → cùng kiểu timestamp UTC.
- **Độ lệch đồng hồ**: B trừ `telemetry_clock_offset_ms` (1500 ms, khai báo trong `sources.json`, lưu ở `bronze.video_files`) **trước** khi đổi sang timestamp. Sau đó `silver_telemetry_classified` gộp bằng `union all by name` và gắn `reject_reason` cho từng dòng.

Bằng chứng đúng: bảng sau chuẩn hoá cho `bat_dau` của B = `2025-10-08 14:30:00.009` (bronze là `…01.509`, tức đã trừ đúng 1,5 s, khớp giờ bắt đầu video 14:30:00), A = `2025-10-07 08:00:00.014`; tốc độ tối đa 71 km/h (A) và 100 km/h (B), hợp lý cho xe chạy đường phố (nếu quên chia 3.6 thì B sẽ lên 360 "km/h"). Verify module 2: 7/7, `clean + quarantined == received` (A: 2968 + 53 = 3021; B: 3181 + 53 = 3234).

| drive | duplicate | speed_missing | speed_out_of_range | tổng cách ly | sạch |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 |
| drive_b | 20 | 30 | 3 | 53 | 3181 |

(3 dòng `speed_out_of_range` của mỗi drive đều là 250 m/s = 900 km/h.) Dòng lỗi vào `silver_telemetry_rejects` (106 dòng, kèm `_batch_id`, `_row_no`) thay vì bị xoá vì: (1) **kiểm toán được** — tổng sạch + cách ly luôn bằng số nhận về, không mất dòng im lặng; (2) **truy được nguồn** để báo lại nhà cung cấp cảm biến; (3) **sửa được** — nếu luật sai (ví dụ ngưỡng 70 m/s quá chặt) thì chỉ cần đổi luật và chạy lại, dòng bị loại không mất; (4) tỉ lệ cách ly theo thời gian là một chỉ số giám sát chất lượng nguồn.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` kiểm tra **độ đầy đủ**: khoảng trống giữa hai mẫu liên tiếp > 1000 ms. Ở dữ liệu của tôi nó bắt **1 khoảng trống mỗi drive** (A: 3073 ms, B: 3124 ms) — cảm biến mất tín hiệu khoảng 3 s. Đây là khiếm khuyết thật của thế giới, dữ liệu còn lại vẫn đúng và dùng được (khung hình rơi vào khoảng trống sẽ bị loại ở bước sau vì `sync_lost`), nên chỉ `warn`: báo cho người vận hành nhưng không dừng pipeline. `in_range` (0–70 m/s) kiểm tra **tính đúng**: một tốc độ vô lý lọt vào silver nghĩa là luật làm sạch đã hỏng, mọi bảng phía sau (gold, mart, nhãn tốc độ cho mô hình) sẽ sai, nên phải `error` để chặn.

Thí nghiệm `break-gate` (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 → 6 dòng 250 m/s (3 mỗi drive) không còn bị cách ly mà lọt vào `silver_telemetry`. `dbt test` ra `PASS=5 WARN=1 ERROR=1`: test `in_range_silver_telemetry_speed_mps__70__0` **bắt được 6 dòng**, `gate_failed: true`. Khôi phục luật: `PASS=6 WARN=1 ERROR=0`. Ý nghĩa: luật làm sạch (trong model) và cổng chất lượng (test) là **hai lớp độc lập**. Khi ai đó sửa nhầm luật, cổng vẫn đứng đó và trong DAG Airflow test `error` làm các task phía sau không chạy, nên dữ liệu xấu không đi tới gold/warehouse. Cổng chỉ hiệu quả khi ngưỡng của nó không bị nới cùng lúc với luật.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
