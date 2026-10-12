# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Đoàn Duy Bách |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602515` |
| GitHub | DuyBach2003 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-03904` |

Ghi chú: MSSV có chữ `A` nên lệnh `int(MSSV) % 26` trong GUIDE báo lỗi. Tôi lấy các chữ số của MSSV: `2202602515 % 26 = 9` → phần tử thứ 9 của `list-trips` là `trip-03904`. Máy tôi có project khác chiếm cổng 8080/5433/3000, nên tôi đổi cổng phía host bằng `docker-compose.override.yml` cục bộ (không commit, `docker-compose.yml` giữ nguyên): Airflow 8081, Postgres 5434, Metabase 3001.

## Kết quả cuối

- `dataset_version` của bạn: `275d97e86de7`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 711 / 146 / 163 (tổng 1020 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).
  Của tôi: `airflow_graph.png`, `minio_lake.png`, `minio_gold_dataset.png`, `metabase_dashboard.png`, `crash_log_trip-03904.png`, `crash_graph_red.png`, `crash_clear_dialog.png`, `crash_graph_after_clear.png`.

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

`leakage.json`: 1020 khung đáng dùng thuộc 26 trip. Chia **theo khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** bị chia cắt qua nhiều split, và cả **26 trip có khung ở cả train lẫn test** (`per_frame_split_trips_in_both_train_and_test: 26`). Chia **theo trip** (gold của pipeline): **0 trip** bị chia cắt (`per_trip_split_trips_straddling: 0`), dbt test `no_straddle` cũng PASS. Split của tôi: train 711 khung/18 trip, val 146/4, test 163/4.

Vì sao điểm đẹp giả tạo: ở 2 khung/giây, hai khung liền nhau của cùng một trip gần như giống hệt (cùng đường, cùng ánh sáng, cùng xe phía trước; thí nghiệm ngưỡng cho thấy hàng trăm cặp liền nhau chỉ khác ≤ 6 bit dHash). Chia theo khung thì với mỗi ảnh test, mô hình gần như chắc chắn đã thấy "anh em sinh đôi" của nó trong train, nên chỉ cần nhớ cảnh chứ không cần tổng quát hoá. Điểm test khi đó đo khả năng **nhớ** chứ không phải khả năng lái trên **cảnh mới**; khi triển khai trên đường chưa từng thấy, độ chính xác tụt mạnh. Chia theo trip (cảnh) mô phỏng đúng tình huống thật: test là những đoạn đường mô hình chưa từng gặp.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version = sha256("frame_id|split" của mọi khung, sắp theo frame_id)[:12]` (`pipeline/publish.py`). Nó là **băm nội dung** nên trả lời đúng câu "hai dataset có giống nhau không?": cùng khung + cùng split ⇒ cùng version; thêm/bớt một khung hoặc một khung đổi split ⇒ version khác. Dùng ngày giờ hay số đếm thì chạy lại cùng dữ liệu cũng ra version mới (không biết hai bản có trùng nhau không), còn hai máy chạy song song có thể cùng số đếm nhưng khác nội dung. Băm nội dung cũng làm việc xuất ảnh idempotent: ảnh trong `gold/datasets/<version>/` đã có thì không chép lại.

Hai lần chạy liên tiếp: `step all --reset`, `step 4` và `cli export-dataset` đều cho **`275d97e86de7`** (1020 khung; train/val/test = 711/146/163), dù `created_at` khác nhau. MSSV được trộn vào thứ tự trip: `row_number() over (partition by drive_id order by md5(trip_id || LAB_STUDENT_ID))`, nên MSSV khác ⇒ trip khác rơi vào train/val/test ⇒ cặp `frame_id|split` khác ⇒ băm khác. Tôi thử đặt `LAB_STUDENT_ID=2024000`: version thành **`7ff13908378c`**, vẫn 1020 khung và vẫn 18/4/4 trip, nhưng số khung train/val/test thành 703/183/134. Đặt lại `2A202602515` thì về đúng `275d97e86de7`. Tức là **tập khung không đổi, chỉ cách chia đổi**, và version phản ánh đúng điều đó; giảng viên chạy lại với MSSV của tôi phải ra đúng `275d97e86de7`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/275d97e86de7/manifest.json` (lần chạy cuối `step all --reset`), phần `lineage`:

```json
"lineage": {"bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
            "silver.frames": 25, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
            "input_video_sha256": {"drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
                                   "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc"}}
```

"Dataset này được tạo từ đâu": từ đúng hai video có sha256 trên; telemetry ở version 0 của `bronze.raw_telemetry_a/b` (bản nạp duy nhất); trips version 0; `bronze.video_files` version 1 (hai commit, mỗi drive một); khung hình từ `silver.frames` **version 25** (26 commit, version 0–25, mỗi trip một partition); cờ chất lượng và join telemetry ở version 0. Kèm theo: `rules` (bỏ `is_black, is_blurry, is_near_dup, not sync_ok`; split theo trip, phân tầng theo drive, thứ tự md5), `by_split` 711/146/163, `trips_by_split` 18/4/4 và `manifest_sha256 = cca836a3…0890` (băm của `manifest.parquet`, danh sách từng khung + split + đường dẫn ảnh). Ở lần chạy trước đó (trước thí nghiệm idempotent và trước reset), `silver.frames` là version 27 nhưng `dataset_version` vẫn `275d97e86de7`: version bảng đổi khi ghi lại, nội dung thì không.

3 tháng sau, để tái tạo đúng dataset cần: (1) sha256 video và các version bảng Delta → đọc lại đúng snapshot bằng **time travel** (miễn chưa `VACUUM` xoá file cũ); (2) `rules` cùng các ngưỡng trong `dbt_project.yml` (`dark_luma=12`, `blur_var=40`, `dup_hamming=2`, `sync_tol_ms=150`, `split_train=0.70`, `split_val=0.15`) và commit git của code; (3) **MSSV `2A202602515`** (salt của split). Kiểm chứng: dataset dựng lại phải có cùng `dataset_version` và `manifest_sha256`. Điểm yếu: manifest chưa ghi commit git, các `vars` của dbt và salt, nên phần (2), (3) phải dựa vào lịch sử repo.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip của tôi là `trip-03904` (map index 9 trong 26 task `silver_frames`). Đặt `LAB_FAIL_ONCE=trip-03904`, `docker compose --profile orchestrate up -d`, trigger `day09_reset` (xanh, xoá cả marker `/opt/work/.failed-*`), rồi trigger `day09_video_pipeline` (run `manual__2026-10-12T09:41:04+00:00`):
- **1 task đỏ**: `silver_frames` map index 9. Log (`report/screenshots/crash_log_trip-03904.png`): `RuntimeError: simulated crash while extracting trip-03904; run it again`. Theo `pipeline/frames.py`, lỗi xảy ra ngay đầu `extract_trip` (`_fail_once`), trước khi giải mã video và trước khi ghi Delta, nên bảng không có commit dở nào; marker `/opt/work/.failed-trip-03904` đảm bảo lần chạy thứ hai không sập.
- **25 task `silver_frames` khác vẫn xanh**: mỗi trip là một task độc lập, một trip hỏng không kéo các trip khác.
- **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` (`report/screenshots/crash_graph_red.png`). Dữ liệu thiếu một trip không đi tiếp tới gold/warehouse.

Tôi **chỉ clear task đỏ** (Clear task trên map index 9, giữ mặc định Downstream): hộp thoại báo **Affected Tasks: 8** = 1 task đỏ + 7 task phía sau (`crash_clear_dialog.png`). Airflow chỉ chạy lại 8 task đó (`attempt=2.log` của map index 9: `Marking task as SUCCESS`); 25 trip đã xong, 3 task bronze và `trip_commands` không chạy lại. Run chuyển xanh: 37/37 task `success` (`crash_graph_after_clear.png`).

Không nhân đôi: sau khi xanh, `lake.silver_frames` có **1246 dòng / 1246 `frame_id` khác nhau**, `trip-03904` có đúng **22 dòng**; `cli history silver.frames` chỉ có một commit `WRITE rows_added=22 predicate trip_id='trip-03904'` cho trip này. `scripts/lab.sh verify`: **27/27**, `dataset_version` **vẫn `275d97e86de7`**, gold 1020 khung. Lý do: task ghi theo kiểu `overwrite` với predicate trip (Câu 4), nên chạy lại bao nhiêu lần cũng ra cùng một trạng thái. Xong bài tập tôi xoá `LAB_FAIL_ONCE` khỏi `.env` và khởi động lại Airflow.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Hiện tại: 2 drive × ~5 phút → 1246 khung, cả DAG ~1 phút trên một container. 500 xe × vài giờ/ngày là gấp hàng chục nghìn lần. Ba chỗ vỡ đầu tiên:

1. **Giải mã video (FFmpeg trong `silver_frames`)** — slide 24 nói decode là nút cổ chai. Bây giờ chạy trên một máy Airflow, `max_active_tis_per_dagrun=1`, `PARALLELISM=2`, 26 task tuần tự. 500 xe là hàng chục nghìn trip/ngày. Thay bằng: tách xử lý video ra một cụm worker co giãn (Kubernetes/Ray/Spark, decode bằng GPU NVDEC), Airflow chỉ gửi việc đi; partition theo `xe/ngày/trip`; chỉ decode các khoảng thời gian cần thiết.
2. **DuckDB một file + dbt trên một máy (silver/gold)** — slide 22, 23: DuckDB đơn tiến trình, file `lakehouse.duckdb` cục bộ, mỗi lần chạy build lại toàn bộ model (`materialized: table`). Với hàng tỷ dòng telemetry/ngày thì không vừa RAM và không chạy được song song. Thay bằng: engine phân tán đọc Delta trực tiếp (Spark/Databricks, Trino) với model **incremental** theo partition ngày/xe; ASOF join theo partition `drive_id`; test dbt chạy trên partition mới, không trên toàn bảng.
3. **Điều phối và ingestion kiểu batch theo file** — slide 12: dynamic task mapping một task/trip làm scheduler Airflow phải quản lý hàng chục nghìn task instance mỗi ngày, metadata DB Postgres và scheduler sẽ nghẽn; ingest sha256 cả file lớn rồi `copy` qua một máy. Thay bằng: xe upload thẳng lên object storage (multipart), sự kiện "file mới" đi qua hàng đợi (Kafka/SQS) để kích hoạt xử lý theo lô (mỗi task một xe-ngày chứ không phải một trip), telemetry đi luồng streaming vào bronze; Airflow chỉ điều phối ở mức lô/ngày, backfill theo partition.

Đi kèm: Postgres + Metabase đọc mart vẫn ổn vì mart là tổng hợp, nhưng `mart_frame_gallery` (mỗi khung một dòng) phải bỏ hoặc lấy mẫu; `copy_object` từng ảnh sang `gold/datasets/<version>/` nên thay bằng manifest trỏ tới ảnh silver (không chép).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi thêm **hai câu hỏi** vào dashboard "Day 9: video data pipeline" (`report/screenshots/metabase_dashboard.png`, hai thẻ cuối):

**Câu hỏi riêng 1: tỷ lệ khung bị loại theo trip** (biểu đồ cột):

```sql
select trip_id, drive_id, split, avg_speed_kmh, frames, frames - frames_kept as dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as drop_pct
from marts.mart_trip_summary
order by drop_pct desc
```

Kết quả: trip bị loại nhiều nhất là `trip-04622` (drive_a, train) **56.7%** (34/60), rồi `trip-03904` 50.0% (11/22), `trip-02086` 46.7%, `trip-08888` 40.0%; ở cuối có trip bị loại 0%. Khi tra ngược ở silver (`cli sql`), cả 34 khung bị loại của `trip-04622` đều do `is_near_dup` (0 khung đen/mờ/mất đồng bộ), dù xe chạy **63–71 km/h**. Đoạn đứng hình được cấy vào là giây 150–156 (thuộc trip trước `trip-02086`), không phải trip này. Xem ảnh thì đó là đường thẳng có tường bê tông dài: ở 2 khung/giây, dHash 64 bit gần như không đổi dù xe đã đi ~9 m. `trip-08888` (0–19 km/h) thì bị loại vì xe đứng yên: có một chuỗi 23 khung trùng liên tiếp. Tức là "khung trùng" gom chung hai hiện tượng khác nhau: xe đứng yên (loại là hợp lý) và cảnh đơn điệu ở tốc độ cao (loại có thể làm mất dữ liệu đường cao tốc).

**Câu hỏi riêng 2 (vận hành): tỷ lệ loại khung theo tốc độ trung bình của trip** (bảng):

```sql
select drive_id,
       case when avg_speed_kmh < 20 then '1) <20 km/h' when avg_speed_kmh < 50 then '2) 20-50 km/h' else '3) >=50 km/h' end as toc_do_tb,
       count(*) as trips, sum(frames) as frames, sum(frames - frames_kept) as dropped,
       round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as drop_pct
from marts.mart_trip_summary group by 1, 2 order by 1, 2
```

drive_a: <20 km/h **32.4%**, 20–50 km/h 22.6%, ≥50 km/h 25.8%; drive_b: 20–50 km/h 10.5%, ≥50 km/h 9.1%. Chạy chậm thì loại nhiều nhất (đúng như dự đoán), nhưng drive_a vẫn mất ~26% ở tốc độ cao, gấp gần 3 lần drive_b, nên tỷ lệ loại phụ thuộc vào **loại cảnh** chứ không chỉ tốc độ. Câu hỏi vận hành rút ra: nếu dataset thiếu cảnh tốc độ cao của drive_a thì nên xem lại ngưỡng `dup_hamming` theo tốc độ (Câu 5).

**Thí nghiệm riêng (điểm cộng): độ nhạy của `sync_tol_ms`.** Không sửa code: tôi tính lại trên `silver.silver_frame_telemetry` số khung "tốt" (không đen/mờ/trùng) được giữ khi đổi ngưỡng đồng bộ:

| `sync_tol_ms` | 50 | 80 | 100 | **150 (đang dùng)** | 300 | 1000 | 5000 |
|---|---|---|---|---|---|---|---|
| khung giữ lại | 539 | 539 | 763 | **1020** | 1020 | 1023 | 1032 |
| khung bị loại vì mất đồng bộ | 495 | 495 | 271 | **14** | 14 | 11 | 2 |

`sync_gap_ms` có trung vị 20 ms, p90 113 ms, p99 120 ms (telemetry ~10 Hz, ASOF lấy mẫu gần nhất trước khung), max 3112 ms (lúc cảm biến mất tín hiệu ~3 s, khớp WARN `max_gap_ms` ở Câu 3). Ngưỡng 150 ms nằm ngay trên đuôi tự nhiên (120 ms): hạ xuống 100 ms mất **257 khung tốt** (25%), còn nâng lên 1000–5000 ms chỉ nhận thêm 3–12 khung, và đó lại chính là các khung nằm trong lỗ mất tín hiệu, gắn với telemetry cũ tới 3 s (nhãn tốc độ/góc lái sai). Vì vậy 150 ms là lựa chọn đúng cho dữ liệu này.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất**: chia theo khung hình làm **26/26** trip rò rỉ sang test, không phải chỉ vài trip. Và `dataset_version` giữ nguyên sau khi task sập rồi chạy lại, dù version Delta của `silver.frames` đã đổi.
- **Chưa rõ**: có nên coi cảnh đường cao tốc đơn điệu là "trùng" không, vì trip-04622 mất 56.7% số khung khi xe chạy khoảng 65 km/h. Có lẽ ngưỡng dHash nên phụ thuộc vào tốc độ.
- **Lỗi pipeline chưa bắt**: (1) cả 26 trip đều là `Day, Clouds`, nhưng không có test nào cảnh báo dataset thiếu cảnh đêm hoặc mưa. (2) Telemetry chỉ được kiểm tra trong khoảng 0–70 m/s, không kiểm tra gia tốc phi vật lý (30 → 0 → 30 m/s giữa hai mẫu liền nhau) hay GPS nhảy. (3) Độ lệch đồng hồ của B lấy theo khai báo chứ không đo, và test chỉ kiểm tra mẫu đầu tiên (±0.5 s), nên đồng hồ trôi dần trong chuyến sẽ không bị phát hiện.
