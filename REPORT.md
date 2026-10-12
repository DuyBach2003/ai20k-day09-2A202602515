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

(trả lời ở đây)

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

(trả lời ở đây)

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
