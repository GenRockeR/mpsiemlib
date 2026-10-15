import unittest
from datetime import UTC, datetime

import requests
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class SourceMonitorTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings
    __begin = None
    __end = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.SOURCE_MONITOR)
        cls.__end = round(datetime.now(tz=UTC).timestamp())
        cls.__begin = cls.__end - 86400

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    # ------------------------- списки источников/форвардеров -------------------------

    def test_get_sources_list(self):
        ret = list(self.__module.get_sources_list(self.__begin, self.__end))
        self.assertNotEqual(len(ret), 0)

    @unittest.skip("Skip test, if no forwarders")
    def test_get_forwarders_list(self):
        ret = list(self.__module.get_forwarders_list(self.__begin, self.__end))
        self.assertNotEqual(len(ret), 0)

    @unittest.skip("Skip test, if no forwarders")
    def test_get_sources_by_forwarder(self):
        # 27.x: форвардер - актив, его id (asset_id) подаётся в forwarderIds
        forwarder = next(self.__module.get_forwarders_list(self.__begin, self.__end))
        forwarder_id = forwarder.get("asset_id") or forwarder.get("id")
        ret = list(
            self.__module.get_sources_by_forwarder(
                forwarder_id, self.__begin, self.__end
            )
        )
        self.assertNotEqual(len(ret), 0)

    def test_get_forwarders_list_invalid_state_filter(self):
        with self.assertRaises(ValueError):
            list(
                self.__module.get_forwarders_list(
                    self.__begin, self.__end, state_filter="no_such_filter"
                )
            )

    def test_get_sources_list_invalid_state_filter(self):
        # stateFilter проверяется до запроса, поэтому итератор не запускается
        with self.assertRaises(ValueError):
            list(
                self.__module.get_sources_list(
                    self.__begin, self.__end, state_filter="no_such_filter"
                )
            )

    # ------------------------- статистика -------------------------

    def test_get_assets_statistics(self):
        ret = self.__module.get_assets_statistics(self.__begin, self.__end)
        for key in (
            "withoutRules",
            "withRules",
            "activityAlerted",
            "flowAlerted",
            "delayAlerted",
            "unidentifiedSources",
        ):
            self.assertIn(key, ret)
            self.assertIsInstance(ret[key], int)

    def test_get_assets_statistics_with_asset_filter(self):
        assets = list(self.__module.get_sources_list(self.__begin, self.__end))
        if not assets:
            self.skipTest("No assets on the stand")
        asset_ids = [assets[0]["asset_id"]]
        ret = self.__module.get_assets_statistics(
            self.__begin, self.__end, asset_ids=asset_ids
        )
        # Фильтр по конкретному активу не может увеличить счётчики правил
        self.assertLessEqual(
            ret["withRules"] + ret["withoutRules"],
            len(assets),
        )

    def test_get_forwarders_statistics(self):
        ret = self.__module.get_forwarders_statistics(self.__begin, self.__end)
        for key in (
            "withoutRules",
            "withRules",
            "noEventsAlerted",
            "delayAlerted",
            "unidentifiedForwarders",
        ):
            self.assertIn(key, ret)
            self.assertIsInstance(ret[key], int)

    # ------------------------- неидентифицированные -------------------------

    def test_get_unidentified_sources(self):
        ret = self.__module.get_unidentified_sources(self.__begin, self.__end)
        self.assertIn("items", ret)
        self.assertIn("totalItems", ret)
        self.assertEqual(len(ret["items"]), ret["totalItems"])
        if ret["items"]:
            source = ret["items"][0].get("source") or {}
            for key in ("id", "vendor", "title"):
                self.assertIn(key, source)

    def test_get_unidentified_forwarders(self):
        ret = self.__module.get_unidentified_forwarders(self.__begin, self.__end)
        self.assertIn("items", ret)
        self.assertIn("totalItems", ret)
        self.assertEqual(len(ret["items"]), ret["totalItems"])

    # ------------------------- графики EPS -------------------------

    def test_get_asset_source_eps_graph(self):
        sources = [
            s
            for s in self.__module.get_sources_list(self.__begin, self.__end)
            if s.get("source_id")
        ]
        if not sources:
            self.skipTest("No sources with source_id on the stand")
        ret = self.__module.get_asset_source_eps_graph(
            sources[0]["asset_id"],
            sources[0]["source_id"],
            self.__begin,
            self.__end,
            granularity="PT30M",
        )
        self.assertIsInstance(ret, list)
        if ret:
            self.assertIn("dateTime", ret[0])
            self.assertIn("value", ret[0])

    def test_get_asset_source_eps_graph_unknown_source(self):
        # Сервер на неизвестные id не ошибается, а отдаёт zerofilled график
        ret = self.__module.get_asset_source_eps_graph(
            "00000000-0000-0000-0000-000000000000",
            "NO_SUCH_SOURCE",
            self.__begin,
            self.__end,
        )
        self.assertIsInstance(ret, list)
        for point in ret:
            self.assertEqual(point["value"], 0.0)

    def test_get_unidentified_source_eps_graph(self):
        unidentified = self.__module.get_unidentified_sources(self.__begin, self.__end)
        if not unidentified["items"]:
            self.skipTest("No unidentified sources on the stand")
        source_id = unidentified["items"][0]["source"]["id"]
        ret = self.__module.get_unidentified_source_eps_graph(
            source_id, "Source", 0, 1439, granularity="PT30M"
        )
        self.assertIsInstance(ret, list)

    def test_get_unidentified_source_eps_graph_invalid_type(self):
        with self.assertRaises(ValueError) as ctx:
            self.__module.get_unidentified_source_eps_graph(
                "NO_SUCH_SOURCE", "NoSuchType", 0, 1439
            )
        self.assertIn("assetSourceType", str(ctx.exception))

    def test_get_unidentified_source_eps_graph_invalid_minutes(self):
        for minutes in (-1, 1440):
            with self.subTest(minutes=minutes), self.assertRaises(ValueError):
                self.__module.get_unidentified_source_eps_graph(
                    "NO_SUCH_SOURCE", "Source", minutes, 1439
                )

    @unittest.skip("Skip test, if no forwarders")
    def test_get_forwarder_eps_graph(self):
        forwarder = next(self.__module.get_forwarders_list(self.__begin, self.__end))
        ret = self.__module.get_forwarder_eps_graph(
            forwarder["asset_id"],
            self.__begin,
            self.__end,
            granularity="PT30M",
        )
        self.assertIsInstance(ret, list)

    # ------------------------- средний EPS -------------------------

    def test_get_average_eps(self):
        self.assertGreaterEqual(self.__module.get_average_eps(0, 1439), 0)

    def test_get_average_eps_by_source(self):
        sources = [
            s
            for s in self.__module.get_sources_list(self.__begin, self.__end)
            if s.get("source_id")
        ]
        if not sources:
            self.skipTest("No sources with source_id on the stand")
        self.assertGreaterEqual(
            self.__module.get_average_eps(0, 1439, sources[0]["source_id"]), 0
        )

    def test_get_average_eps_invalid_minutes(self):
        for minutes in (-1, 1440):
            with self.subTest(minutes=minutes), self.assertRaises(ValueError):
                self.__module.get_average_eps(minutes, 1439)

    # ------------------------- экспорт в CSV -------------------------

    def assert_csv(self, csv_text):
        # BOM снимается на стороне модуля
        self.assertFalse(csv_text.startswith("\ufeff"))
        self.assertTrue(csv_text.strip())
        self.assertIn(";", csv_text.splitlines()[0])

    def test_export_assets(self):
        self.assert_csv(self.__module.export_assets(self.__begin, self.__end))

    def test_export_assets_specified(self):
        assets = list(self.__module.get_sources_list(self.__begin, self.__end))
        if not assets:
            self.skipTest("No assets on the stand")
        self.assert_csv(
            self.__module.export_assets_specified([{"assetId": assets[0]["asset_id"]}])
        )

    def test_export_assets_invalid_state_filter(self):
        with self.assertRaises(ValueError):
            self.__module.export_assets(
                self.__begin, self.__end, state_filter="no_such_filter"
            )

    def test_export_unidentified_sources(self):
        self.assert_csv(
            self.__module.export_unidentified_sources(self.__begin, self.__end)
        )

    def test_export_unidentified_forwarders(self):
        self.assert_csv(
            self.__module.export_unidentified_forwarders(self.__begin, self.__end)
        )

    def test_export_forwarders(self):
        # На стенде форвардеров может не быть - тогда CSV с одной шапкой
        csv_text = self.__module.export_forwarders(self.__begin, self.__end)
        self.assert_csv(csv_text)

    def test_export_forwarders_specified(self):
        # Форвардеров на стенде может не быть - сервер всё равно отдаёт шапку
        assets = list(self.__module.get_sources_list(self.__begin, self.__end))
        if not assets:
            self.skipTest("No assets on the stand")
        self.assert_csv(
            self.__module.export_forwarders_specified(
                [{"assetId": assets[0]["asset_id"]}]
            )
        )

    def test_export_forwarders_specified_empty_list(self):
        # Пустой список контракт разрешает, но сервер (R28.0) отвечает 400
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.export_forwarders_specified([])
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_export_forwarders_invalid_state_filter(self):
        with self.assertRaises(ValueError):
            self.__module.export_forwarders(
                self.__begin, self.__end, state_filter="no_such_filter"
            )


if __name__ == "__main__":
    unittest.main()
