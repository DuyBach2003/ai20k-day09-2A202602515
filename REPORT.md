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

Tôi chạy `exp idempotency --trip trip-03904` (chính trip dùng cho bài tập sập). Bảng `silver.frames` trước: **1246 dòng**, dấu vân tay `cb6118dbb0b8`; sau **hai lần** chạy lại partition đó: vẫn **1246 dòng**, vân tay `cb6118dbb0b8` (`unchanged: true`). `cli history silver.frames` cho thấy hai commit mới (version 26 và 27), mỗi commit là `WRITE` với `predicate: trip_id = 'trip-03904'`, `rows_added: 22` — tức thay đúng 22 dòng của trip đó, các trip khác không bị chạm. Với cách ghi sai `append` chạy 2 lần, trip 22 khung thành **44 dòng** (nhân đôi), và mọi bảng phía sau (gold, gallery) sẽ có khung trùng.

Delta `overwrite` với `predicate` an toàn hơn "xoá rồi ghi lại" vì **xoá dòng cũ và thêm dòng mới nằm trong cùng một commit nguyên tử** vào `_delta_log`. Nếu task sập giữa chừng (như bài tập bước 5), file Parquet đã ghi dở không được commit nên người đọc vẫn thấy phiên bản cũ đầy đủ; chạy lại chỉ cần ghi đè lại đúng partition. Với "DELETE rồi INSERT" là hai thao tác riêng: sập sau DELETE thì trip đó **biến mất** khỏi bảng, người đọc đồng thời thấy dữ liệu thiếu, và nếu retry chỉ INSERT thì dễ nhân đôi. Ngoài ra Delta giữ lịch sử nên có thể time travel về version trước nếu lần ghi mới sai.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Khung đen** (`brightness < 12`, `dark_luma`) là ảnh hỏng: camera/cảm biến mất tín hiệu, không có thông tin gì — độ sáng thực tế = 0.0 ở cả hai drive (A: 6 khung, B: 4 khung). Pipeline gắn `is_black` và **loại khỏi gold**; còn có test `max_share` dừng pipeline nếu > 10% khung đen (camera hỏng). **Khung tối** (`12 ≤ brightness < 40`, `dark_soft`) là điều kiện thật: đường hầm, bóng râm, chạng vạng (B có độ sáng 12.8–39.6). Pipeline chỉ gắn `is_dark` và **giữ lại** (A: 4, B: 61). Tôi đồng ý: mô hình xe tự lái phải học được cả cảnh tối, bỏ chúng sẽ làm dataset thiên lệch về cảnh sáng và mô hình hỏng đúng ở tình huống nguy hiểm nhất. Riêng khung mờ chỉ tính khi không đen (`is_blurry` cần `brightness ≥ 12`) để một khung không bị đếm hai lý do.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 60 | 8 |
| 2 | 149 | 52 |
| 6 | 363 | 244 |

(trên tổng 602 khung A và 644 khung B; ngưỡng 1/4/8: A 109/270/438, B 24/137/350.) Tôi giữ **ngưỡng 2**: ngưỡng 0 bỏ sót đứng hình vì nén video làm vài bit dHash thay đổi dù cảnh không đổi (verify bắt được 11/12 khung đứng hình ở ngưỡng 2); ngưỡng 6 loại 60% khung của A và 38% của B, tức loại cả những khung khác nhau thật. Từ 2 lên 4, số khung trùng của B tăng gần 3 lần (52 → 137), dấu hiệu bắt đầu ăn vào khung thật. Ngưỡng phụ thuộc tốc độ: ở 2 khung/giây, xe đứng yên/chạy chậm thì hai khung liền nhau gần như giống hệt — trong dữ liệu của tôi, khung ở tốc độ 0–2 m/s bị coi là trùng **59.3%** (54/91, toàn ở drive A), còn 2–15 m/s chỉ ~8–10%. Ở ≥15 m/s tỉ lệ lại lên 14.4% vì A có 6 s camera đứng hình và cảnh đường cao tốc ít chi tiết. Nếu chỉnh, tôi sẽ cho ngưỡng thay đổi theo tốc độ (chặt hơn khi xe đứng yên, hoặc dùng thêm `speed_mps≈0` để chỉ lấy mẫu thưa hơn), và giữ ngưỡng thấp khi xe chạy nhanh.

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
