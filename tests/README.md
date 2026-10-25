# Тесты async_bot

Запуск из корня репозитория (нужны пакеты из requirements.txt: ccxt==4.2.14, aiohttp, numpy, pyTelegramBotAPI, websockets):

```
python3 -m unittest discover -s tests -v
```

Особенности:
- тесты не ходят в сеть и не пишут в боевую БД: `TRADES_DB` подменяется временным файлом, `tg()` и обращения к бирже — заглушки;
- `SMC2_ENABLED` по умолчанию `false` (v93); тесты сканера SMC2 запускать с `SMC2_ENABLED=true`;
- до v94 регрессионные тесты хранились вне git и в репозиторий не попали; здесь тесты v94 (`test_v94_health.py`) и v95 (`test_v95_edge.py`, `/edge_audit`), v96 (`test_v96_channels.py`), v97 (`test_v97_research.py`), v98 (`test_v98_carry.py`), v99 (`test_v99_ma.py`), v100–v101 (`test_v100_ma2.py`, проба v2), v102 (`test_v102_digest_probe.py`), v103 (`test_v103_factory.py`, `test_v103_e2e.py`; общие фикстуры — `factory_fixtures.py`). Новые тесты добавлять сюда же.
