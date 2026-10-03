import unittest
from random import choice
from string import ascii_lowercase

from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class FiltersTestCase(unittest.TestCase):
    """Тесты модуля Filters.

    Тесты, создающие объекты, самодостаточны: создают собственную папку в
    пользовательском корне и удаляют её через addCleanup (DELETE папки
    каскадно убирает её фильтры - измерено на стенде). SDK умеет удалять и
    папки, и фильтры, поэтому raw-запросы в тестах не нужны.
    """

    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings

    # Единственный родитель, где создание разрешено: в system/shared папках
    # сервер отвечает 403 forbidden.error (измерено).
    _USER_ROOT_NAME = "Пользовательские фильтры"
    # Валидный PDQL для контракта v3: where(...)/qsearch(...) как pipe-стадия
    # отклоняются Events.Filters.Validation.Pdql.Pipe.Error (измерено).
    _PDQL_V3 = "select(time, event_src.host) | sort(time desc)"
    _PDQL_V3_ALT = "select(time) | sort(time desc)"

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.FILTERS)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _unique(self, prefix: str) -> str:
        return prefix + "".join(choice(ascii_lowercase) for _ in range(12))

    def _user_root_id(self) -> str:
        folders = self.__module.get_folders_list()
        for folder_id, folder in folders.items():
            if folder.get("name") == self._USER_ROOT_NAME:
                return folder_id
        self.skipTest(f"На стенде нет папки {self._USER_ROOT_NAME!r}")
        return ""

    def _create_probe_folder(self) -> str:
        """Создать папку в пользовательском корне и зарегистрировать удаление."""
        folder_id = self.__module.create_event_filter_folder(
            folder_name=self._unique("sdk-dir-"), parent_id=self._user_root_id()
        )
        self.addCleanup(self.__module.delete_event_filter_folder, folder_id)
        return folder_id

    def _create_probe_filter(
        self, folder_id: str | None = None, pdql: str | None = None
    ) -> tuple[str, str, str]:
        """Создать фильтр (v3) и вернуть (filter_id, name, folder_id)."""
        folder_id = folder_id or self._create_probe_folder()
        filter_name = self._unique("sdk-f-")
        filter_id = self.__module.create_event_filter(
            filter_name=filter_name,
            folder_id=folder_id,
            pdql_query=pdql or self._PDQL_V3,
        )
        self.addCleanup(self.__module.delete_event_filter, filter_id)
        return filter_id, filter_name, folder_id

    def _fresh_module(self):
        """Свежий экземпляр модуля: читает иерархию заново, без общего кэша.

        close() не зовём: get_module без creds переиспользует ту же сессию
        авторизации, что и модуль класса.
        """
        return self.__mpsiemworker.get_module(ModuleNames.FILTERS)

    # ------------------------------------------------------------------ #
    # справочные запросы
    # ------------------------------------------------------------------ #
    def test_get_folders_list(self):
        folders = self.__module.get_folders_list()
        self.assertTrue(isinstance(folders, dict) and len(folders) > 0)
        for folder_id, folder in folders.items():
            self.assertTrue(isinstance(folder_id, str) and len(folder_id) > 0)
            self.assertIn("name", folder)
            self.assertIn("source", folder)
        # Корневые папки обязаны присутствовать
        self.assertTrue(
            any(f.get("parent_id") is None for f in folders.values()),
            "в иерархии нет ни одной корневой папки",
        )

    def test_get_folders_list_returns_copy(self):
        """Мутация результата не должна портить внутренний кэш модуля."""
        folders = self.__module.get_folders_list()
        victim = next(iter(folders))
        folders[victim] = {"parent_id": "HACKED", "name": "HACKED", "source": "user"}

        self.assertNotEqual(
            self.__module.get_folders_list().get(victim, {}).get("name"), "HACKED"
        )

    def test_get_filters_list(self):
        filters = self.__module.get_filters_list()
        folders = self.__module.get_folders_list()
        self.assertTrue(isinstance(filters, dict) and len(filters) > 0)
        for flt in filters.values():
            self.assertIn("name", flt)
            self.assertIn("source", flt)
            # Фильтр либо висит на папке из той же иерархии, либо лежит в корне
            # (измерено: системный фильтр "Все события" вложен в корень дерева)
            if flt.get("folder_id") is not None:
                self.assertIn(flt["folder_id"], folders)

    def test_get_filter_info(self):
        filter_id = next(iter(self.__module.get_filters_list()))
        info = self.__module.get_filter_info(filter_id)
        self.assertTrue(isinstance(info, dict) and len(info) > 0)
        self.assertIn("name", info)
        self.assertIn("folder_id", info)
        self.assertIn("removed", info)
        # На ядрах 26.1+ get_filter_info идёт контрактом v3
        self.assertIn("pdqlQuery", info)

    # ------------------------------------------------------------------ #
    # папки: создание, чтение, изменение, удаление
    # ------------------------------------------------------------------ #
    def test_folder_lifecycle(self):
        """Создание -> info -> переименование -> удаление (с каскадом)."""
        folder_id = self.__module.create_event_filter_folder(
            folder_name=self._unique("sdk-dir-"), parent_id=self._user_root_id()
        )
        # Cleanup выполняется LIFO и переживает падение assert'ов: папка
        # уберётся вместе с вложенным фильтром, даже если тест не дошёл до
        # явного удаления.
        self.addCleanup(self.__module.delete_event_filter_folder, folder_id)

        parent_id = self._user_root_id()
        info = self.__module.get_event_filter_folder_info(folder_id)
        self.assertEqual(info.get("folderId"), folder_id)
        self.assertEqual(info.get("type"), "user")
        self.assertEqual(info.get("filtersCount"), 0)

        new_name = self._unique("sdk-ren-")
        self.__module.update_event_filter_folder(folder_id, new_name, parent_id)
        self.assertEqual(
            self.__module.get_event_filter_folder_info(folder_id).get("name"), new_name
        )

        # Кэш иерархии после изменения должен показать новое имя
        self.assertEqual(
            self.__module.get_folders_list().get(folder_id, {}).get("name"), new_name
        )

        # Вложенный фильтр удалится вместе с папкой
        self.__module.create_event_filter(
            filter_name=self._unique("sdk-cascade-"),
            folder_id=folder_id,
            pdql_query=self._PDQL_V3,
        )
        self.__module.delete_event_filter_folder(folder_id)

        module = self._fresh_module()
        self.assertNotIn(folder_id, module.get_folders_list())
        self.assertTrue(
            all(
                f.get("folder_id") != folder_id
                for f in module.get_filters_list().values()
            ),
            "фильтр пережил удаление родительской папки",
        )

    def test_update_event_filter_folder_moves_folder(self):
        """PUT с другим родителем переносит папку, а не только переименовывает."""
        folder_id = self._create_probe_folder()
        new_parent_id = self._create_probe_folder()
        parent_id = self._user_root_id()
        new_name = self._unique("sdk-mv-")

        self.__module.update_event_filter_folder(folder_id, new_name, new_parent_id)

        moved = self.__module.get_folders_list().get(folder_id, {})
        self.assertEqual(moved.get("name"), new_name)
        self.assertEqual(moved.get("parent_id"), new_parent_id)
        self.assertNotEqual(moved.get("parent_id"), parent_id)
        self.assertIn(
            folder_id,
            [
                child.get("folderId")
                for child in self.__module.get_event_filter_folders_in_folder(
                    new_parent_id
                )
            ],
        )

    def test_delete_is_idempotent(self):
        """Повторное удаление не должно ломать addCleanup (измерено - 200)."""
        folder_id = self._create_probe_folder()
        filter_id, _, _ = self._create_probe_filter(folder_id=folder_id)

        self.__module.delete_event_filter(filter_id)
        self.__module.delete_event_filter(filter_id)
        self.__module.delete_event_filter_folder(folder_id)
        self.__module.delete_event_filter_folder(folder_id)

        module = self._fresh_module()
        self.assertNotIn(folder_id, module.get_folders_list())
        self.assertNotIn(filter_id, module.get_filters_list())

    def test_get_event_filter_folders_in_folder(self):
        parent_id = self._create_probe_folder()
        child_name = self._unique("sdk-child-")
        child_id = self.__module.create_event_filter_folder(child_name, parent_id)
        self.addCleanup(self.__module.delete_event_filter_folder, child_id)

        children = self.__module.get_event_filter_folders_in_folder(parent_id)
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0].get("folderId"), child_id)
        self.assertEqual(children[0].get("name"), child_name)
        self.assertEqual(self.__module.get_event_filter_folders_in_folder(child_id), [])

    def test_get_event_filters_in_folder(self):
        _, filter_name, folder_id = self._create_probe_filter()

        content = self.__module.get_event_filters_in_folder(folder_id)
        self.assertEqual(len(content), 1)
        self.assertEqual(content[0].get("name"), filter_name)
        self.assertEqual(content[0].get("folderId"), folder_id)

    def test_created_objects_appear_in_hierarchy(self):
        folder_name = self._unique("sdk-dir-")
        filter_name = self._unique("sdk-hier-")
        folder_id = self.__module.create_event_filter_folder(
            folder_name=folder_name, parent_id=self._user_root_id()
        )
        self.addCleanup(self.__module.delete_event_filter_folder, folder_id)
        filter_id = self.__module.create_event_filter(
            filter_name=filter_name, folder_id=folder_id, pdql_query=self._PDQL_V3
        )
        self.addCleanup(self.__module.delete_event_filter, filter_id)

        module = self._fresh_module()
        self.assertEqual(
            module.get_folders_list().get(folder_id, {}).get("name"), folder_name
        )
        self.assertEqual(
            module.get_filters_list().get(filter_id, {}).get("folder_id"), folder_id
        )

    # ------------------------------------------------------------------ #
    # фильтры: создание и чтение (контракт v3)
    # ------------------------------------------------------------------ #
    def test_create_event_filter_v3_str(self):
        folder_id = self._create_probe_folder()
        filter_name = self._unique("sdk-f3-")

        filter_id = self.__module.create_event_filter(
            filter_name=filter_name, folder_id=folder_id, pdql_query=self._PDQL_V3
        )
        self.addCleanup(self.__module.delete_event_filter, filter_id)

        info = self.__module.get_filter_info(filter_id)
        self.assertEqual(info.get("name"), filter_name)
        self.assertEqual(info.get("folder_id"), folder_id)
        self.assertEqual(info.get("removed"), False)
        self.assertEqual(info.get("source"), "user")
        self.assertEqual(info.get("pdqlQuery"), self._PDQL_V3)

    def test_create_event_filter_v3_method(self):
        folder_id = self._create_probe_folder()
        filter_name = self._unique("sdk-f3m-")

        filter_id = self.__module.create_event_filter_v3(
            filter_name=filter_name, folder_id=folder_id, pdql_query=self._PDQL_V3
        )
        self.addCleanup(self.__module.delete_event_filter, filter_id)

        info = self.__module.get_filter_info_v3(filter_id)
        self.assertEqual(info.get("name"), filter_name)
        self.assertEqual(info.get("folder_id"), folder_id)

    # ------------------------------------------------------------------ #
    # фильтры: создание и чтение (контракт v2)
    # ------------------------------------------------------------------ #
    def test_create_event_filter_v2_dict(self):
        folder_id = self._create_probe_folder()
        filter_name = self._unique("sdk-f2-")
        parts = {"select": ["time"], "where": "normalized = true"}

        filter_id = self.__module.create_event_filter(
            filter_name=filter_name, folder_id=folder_id, pdql_query=parts
        )
        self.addCleanup(self.__module.delete_event_filter, filter_id)

        info = self.__module.get_filter_info_v2(filter_id)
        self.assertEqual(info.get("name"), filter_name)
        self.assertEqual(info.get("folder_id"), folder_id)
        self.assertEqual(info.get("removed"), False)
        self.assertEqual(info["query"].get("select"), ["time"])
        self.assertEqual(info["query"].get("where"), "normalized = true")

    def test_create_event_filter_v2_does_not_mutate_params(self):
        folder_id = self._create_probe_folder()
        parts = {"select": ["time"], "where": "normalized = true"}

        filter_id = self.__module.create_event_filter(
            filter_name=self._unique("sdk-mut-"),
            folder_id=folder_id,
            pdql_query=parts,
        )
        self.addCleanup(self.__module.delete_event_filter, filter_id)

        self.assertEqual(set(parts), {"select", "where"})

    # ------------------------------------------------------------------ #
    # фильтры: изменение
    # ------------------------------------------------------------------ #
    def test_update_event_filter_v3(self):
        filter_id, _, folder_id = self._create_probe_filter()
        new_name = self._unique("sdk-up-")

        self.__module.update_event_filter_v3(
            filter_id=filter_id,
            filter_name=new_name,
            folder_id=folder_id,
            pdql_query=self._PDQL_V3_ALT,
        )

        info = self.__module.get_filter_info_v3(filter_id)
        self.assertEqual(info.get("name"), new_name)
        self.assertEqual(info.get("pdqlQuery"), self._PDQL_V3_ALT)

        # Кэш иерархии после изменения должен показать новое имя
        self.assertEqual(
            self.__module.get_filters_list().get(filter_id, {}).get("name"), new_name
        )

    def test_update_event_filter_v2(self):
        folder_id = self._create_probe_folder()
        filter_id = self.__module.create_event_filter_v2(
            filter_name=self._unique("sdk-f2-"),
            folder_id=folder_id,
            params={"select": ["time"], "where": "normalized = true"},
        )
        self.addCleanup(self.__module.delete_event_filter, filter_id)
        new_name = self._unique("sdk-f2-up-")

        self.__module.update_event_filter_v2(
            filter_id=filter_id,
            filter_name=new_name,
            folder_id=folder_id,
            params={"select": ["time", "text"], "where": "normalized = false"},
        )

        info = self.__module.get_filter_info_v2(filter_id)
        self.assertEqual(info.get("name"), new_name)
        self.assertEqual(info["query"].get("select"), ["time", "text"])
        self.assertEqual(info["query"].get("where"), "normalized = false")

    # ------------------------------------------------------------------ #
    # фильтры: удаление
    # ------------------------------------------------------------------ #
    def test_delete_event_filter(self):
        filter_id, _, _ = self._create_probe_filter()

        self.__module.delete_event_filter(filter_id)

        module = self._fresh_module()
        self.assertNotIn(filter_id, module.get_filters_list())
        # Удалённый фильтр помечен, а не стёрт (измерено)
        by_ids = self.__module.get_filters_by_ids([filter_id], with_removed=True)
        self.assertEqual(len(by_ids), 1)
        self.assertEqual(by_ids[0].get("isRemoved"), True)
        self.assertEqual(
            self.__module.get_filters_by_ids([filter_id]),
            [],
            "удалённый фильтр вернулся без with_removed",
        )

    # ------------------------------------------------------------------ #
    # выборки фильтров
    # ------------------------------------------------------------------ #
    def test_get_filters_by_ids(self):
        first_id, first_name, _ = self._create_probe_filter()
        second_id, second_name, _ = self._create_probe_filter(pdql=self._PDQL_V3_ALT)

        found = self.__module.get_filters_by_ids([first_id, second_id])
        self.assertEqual(len(found), 2)
        self.assertEqual({f.get("name") for f in found}, {first_name, second_name})
        self.assertEqual(self.__module.get_filters_by_ids([]), [])

    def test_get_default_filter(self):
        default_filter = self.__module.get_default_filter()
        self.assertTrue(isinstance(default_filter, dict) and len(default_filter) > 0)
        self.assertIn("id", default_filter)
        self.assertIn("pdqlQuery", default_filter)
        self.assertEqual(default_filter.get("isRemoved"), False)

    # ------------------------------------------------------------------ #
    # фильтры по тегу
    # ------------------------------------------------------------------ #
    def test_get_filters_by_tag_unknown_tag(self):
        """Неизвестный тег - пустой список, а не ошибка (измерено: 200 [])."""
        found = self.__module.get_filters_by_tag(self._unique("sdk-notag-"))
        self.assertEqual(found, [])

    def test_get_filters_by_tag_created_filter_untagged(self):
        """Созданный SDK фильтр не попадает в выборку по тегу.

        Events API не умеет присваивать фильтрам теги: `tags` в теле POST/PUT
        сервер молча игнорирует (измерено на R27.6), поэтому тегующая
        подсистема на стенде отсутствует и выборка обязана быть пустой -
        иначе в неё утекут нетегированные фильтры.
        """
        _, filter_name, _ = self._create_probe_filter()

        self.assertEqual(self.__module.get_filters_by_tag(filter_name), [])
        self.assertEqual(self.__module.get_filters_by_tag(filter_name, True), [])

    def test_get_filters_by_tag_rejects_blank(self):
        """Пустой тег должен отсекаться локально (сервер на него - 400)."""
        for blank in ("", "   ", "\t"):
            with self.assertRaises(ValueError):
                self.__module.get_filters_by_tag(blank)

    # ------------------------------------------------------------------ #
    # выбор контракта по версии ядра
    # ------------------------------------------------------------------ #
    def test_pdql_string_requires_r261(self):
        """PDQL-строка на ядре старше 26.1 должна падать внятной ошибкой.

        Версия понижается напрямую: отдельного стенда <26.1 нет, а ветка
        выбора контракта должна быть закрыта регрессией.
        """
        saved = self.__module._Filters__core_release
        self.__module._Filters__core_release = (26, 0)
        self.addCleanup(setattr, self.__module, "_Filters__core_release", saved)
        with self.assertRaises(ValueError):
            self.__module.create_event_filter(
                filter_name=self._unique("sdk-x-"),
                folder_id=self._user_root_id(),
                pdql_query=self._PDQL_V3,
            )


if __name__ == "__main__":
    unittest.main()
