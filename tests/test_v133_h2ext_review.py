"""v133 (§2.63): команда /h2ext_review — отправка файла h2ext_universe_review.txt документом. Сеть не используется (sendDocument подменён)."""
import asyncio
import os
import tempfile
import time
import unittest

import _common
import disk_usage as DU
from _common import a

ROOT = _common.ROOT
NAME = 'h2ext_universe_review.txt'


def run(c):
    return asyncio.run(c)


def put(root, rel, text='x', mtime=None):
    p = os.path.join(root, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    open(p, 'w', encoding='utf-8').write(text)
    if mtime:
        os.utime(p, (mtime, mtime))
    return p


class Find(unittest.TestCase):
    def test_root_and_first_level_subdirs_newest_wins(self):
        root = tempfile.mkdtemp()
        put(root, f'logs/{NAME}', 'старый', time.time() - 1000)
        new = put(root, f'xdata/{NAME}', 'новый', time.time() - 10)
        deep = put(root, f'xdata/a/b/{NAME}', 'слишком глубоко', time.time())
        p, seen = DU.find_named_file(NAME, [root, root, '/нет/такого', None])
        self.assertEqual(p, new)
        self.assertEqual(seen, [root])
        self.assertNotEqual(p, deep)
        put(root, NAME, 'в корне', time.time())
        self.assertEqual(DU.find_named_file(NAME, [root])[0], os.path.join(root, NAME))

    def test_missing_and_unsafe_names(self):
        root = tempfile.mkdtemp()
        put(root, 'other.txt')
        self.assertEqual(DU.find_named_file(NAME, [root]), (None, [root]))
        for bad in ('../etc/passwd', 'a/b.txt', '/etc/passwd', ''):
            self.assertEqual(DU.find_named_file(bad, [root])[0], None)
        self.assertEqual(DU.find_named_file('other.txt', [root])[0], os.path.join(root, 'other.txt'))     # имя зашито в вызове команды, не приходит от пользователя


class Command(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        _db, self.sent = _common.fresh_state()
        self.docs = []

        async def fake_doc(path, caption='', timeout=20):
            self.docs.append((path, caption, timeout, open(path, encoding='utf-8').read()))
            return True
        self.orig = (a.tg_send_document, a._disk_params)
        a.tg_send_document = fake_doc
        r = self.root
        a._disk_params = lambda: {'root': r, 'xdata': os.path.join(r, 'xdata'), 'leadlag': os.path.join(r, 'leadlag'), 'jobs': os.path.join(r, 'research_jobs'), 'factory': os.path.join(r, 'factory'),
                                  'll_dir': os.path.join(r, 'leadlag'), 'vision': '', 'edge': '', 'audit_cache': '', 'vrp': '', 'battery': ''}

    def tearDown(self):
        a.tg_send_document, a._disk_params = self.orig

    def test_sends_found_file_as_document(self):
        put(self.root, f'xdata/{NAME}', 'обзор вселенной\nстрока 2')
        run(a.h2ext_review_cmd())
        self.assertEqual(len(self.docs), 1)
        path, caption, timeout, body = self.docs[0]
        self.assertEqual(os.path.basename(path), NAME)
        self.assertEqual(body, 'обзор вселенной\nстрока 2')
        self.assertIn(NAME, caption)
        self.assertTrue(timeout >= 60)                                              # большой файл — увеличенный таймаут
        self.assertEqual(self.sent, [])

    def test_missing_file_message_lists_directories(self):
        os.makedirs(os.path.join(self.root, 'xdata'))
        run(a.h2ext_review_cmd())
        self.assertEqual(self.docs, [])
        self.assertTrue(any('не найден' in m and self.root in m and NAME in m for m in self.sent))

    def test_too_big_file_not_sent(self):
        p = put(self.root, NAME, 'x')
        orig = os.path.getsize
        os.path.getsize = lambda q: DU.TG_MAX_BYTES + 1 if q == p else orig(q)
        try:
            run(a.h2ext_review_cmd())
        finally:
            os.path.getsize = orig
        self.assertEqual(self.docs, [])
        self.assertTrue(any('50 МБ' in m for m in self.sent))

    def test_send_failure_reported(self):
        put(self.root, NAME, 'x')

        async def bad(path, caption='', timeout=20):
            return False
        a.tg_send_document = bad
        run(a.h2ext_review_cmd())
        self.assertTrue(any('не удалось отправить' in m for m in self.sent))

    def test_command_registered_no_path_arguments_and_no_other_files(self):
        pool, allv = a.command_pools()
        self.assertIn('/h2ext_review', allv)
        self.assertIsNone(a.command_hint('/h2ext_review'))
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("cmd == '/h2ext_review':", src)                               # без «startswith»: аргументы не принимаются
        self.assertIn('/h2ext_review —', src)
        seg = src[src.index('async def h2ext_review_cmd'):src.index('async def disk_cleanup_cmd')]
        self.assertIn('H2EXT_REVIEW_FILE', seg)
        self.assertNotIn('args', seg.split('"""')[2])
        self.assertEqual(a.H2EXT_REVIEW_FILE, NAME)


class Docs(unittest.TestCase):
    def test_records(self):
        src = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        for needle in ('### 2.63.', '| v133 |', '/h2ext_review'):
            self.assertIn(needle, src)
        self.assertIn('2026-11-26-v133', [x[0] for x in a.CHANGELOG])
        self.assertIn('| v133 |', src)


if __name__ == '__main__':
    unittest.main()
