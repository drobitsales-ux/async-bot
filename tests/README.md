
- v116: `test_v116_h2x1.py` — H2-репликация (§2.47), X1 (§2.48), `/exec_check` на синтетике и фейковых клиентах.
- v117: `test_v117_decouple.py` — развязка публичных данных BingX и ключа (§2.49): keyless `exchange_pub`, алерт 100413, дайджест воркера, версии.
- v129: `test_v129_h2ext_leadlag.py` — клиенты Gate.io/MEXC/Bitget, проба доступа, H2EXT/H2F (§2.59), сбор и отчёт расхождений цен OKX↔BingX (§2.60).
- v130: `test_v130_disk_resume_b3.py` — маркер H2EXT-VENUES, диск до /h2_ext load, /disk_usage и /disk_cleanup, возобновление lead-lag и разрывы, правило двух тиков B3.
- v131: `test_v131_bitget_classify_dataend.py` — Bitget: дефект запроса ≠ «пусто», формы запроса, /venue_probe; конец кэша BingX/OKX; маркер исключения.
- v132: `test_v132_lean_memory.py` — лёгкая панель H2 (равенство полной, память под RLIMIT_AS 300 МБ на синтетике прод-размера), /disk_usage по не загруженным, audit_cache/vrp, форма запроса Bitget в отчёте загрузки.
- v133: `test_v133_h2ext_review.py` — /h2ext_review: поиск и отправка файла h2ext_universe_review.txt документом.
- v134: `test_v134_ext_filter.py` — механическое исключение не-крипто (б, в, г), отчёт по пунктам и биржам, файл-обзор вселенной, /contract_probe.
- v135: `test_v135_meta_rule.py` — пункт а по выводу /contract_probe: поля Gate/Bitget/MEXC/OKX, задание h2_coins (поля + монеты), отчёт и файл-обзор.
