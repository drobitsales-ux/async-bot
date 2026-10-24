# Тесты async_bot

Запуск из корня репозитория (нужны пакеты из requirements.txt: ccxt==4.2.14, aiohttp, numpy, pyTelegramBotAPI, websockets):

```
python3 -m unittest discover -s tests -v
```

Особенности:
- тесты не ходят в сеть и не пишут в боевую БД: `TRADES_DB` подменяется временным файлом, `tg()` и обращения к бирже — заглушки;
- `SMC2_ENABLED` по умолчанию `false` (v93); тесты сканера SMC2 запускать с `SMC2_ENABLED=true`;
- до v94 регрессионные тесты хранились вне git и в репозиторий не попали; здесь — только тесты v94 (`test_v94_health.py`). Новые тесты добавлять сюда же.
