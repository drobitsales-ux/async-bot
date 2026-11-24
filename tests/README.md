
- v116: `test_v116_h2x1.py` — H2-репликация (§2.47), X1 (§2.48), `/exec_check` на синтетике и фейковых клиентах.
- v117: `test_v117_decouple.py` — развязка публичных данных BingX и ключа (§2.49): keyless `exchange_pub`, алерт 100413, дайджест воркера, версии.
- v129: `test_v129_h2ext_leadlag.py` — клиенты Gate.io/MEXC/Bitget, проба доступа, H2EXT/H2F (§2.59), сбор и отчёт расхождений цен OKX↔BingX (§2.60).
- v130: `test_v130_disk_resume_b3.py` — маркер H2EXT-VENUES, диск до /h2_ext load, /disk_usage и /disk_cleanup, возобновление lead-lag и разрывы, правило двух тиков B3.
