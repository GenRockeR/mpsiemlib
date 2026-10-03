from random import choice
from string import ascii_lowercase, ascii_uppercase


def gen_uppercase_string(strlen: int) -> str:
    return "unittest_" + "".join(choice(ascii_uppercase) for _ in range(strlen))


def gen_lowercase_string(strlen: int) -> str:
    return "unittest_" + "".join(choice(ascii_lowercase) for _ in range(strlen))


KB_CONTENT_KINDS = {
    "CorrelationRule": "Correlation",
    "AggregationRule": "Aggregation",
    "EnrichmentRule": "Enrichment",
    "NormalizationRule": "Normalization",
}


def _depth(entry_id, tree):
    """Глубина узла дерева {'id': {'parent_id': ...}} (корень - 1)."""
    depth = 0
    seen = set()
    current = entry_id
    while current and current in tree and current not in seen:
        seen.add(current)
        current = tree[current].get("parent_id")
        depth += 1
    return depth


# Префиксы имён тестового мусора, которые создаёт KB-тестов suite
# (gen_uppercase_string / gen_lowercase_string).
KB_TRASH_PREFIXES = ("unittest_",)


def clean_kb_trash(module, db_name, prefixes=KB_TRASH_PREFIXES):
    """Уборка тестового мусора KB по префиксам имён.

    Вызывать из tearDownClass: подчищает контент/папки/наборы, имена которых
    начинаются с префиксов тестового мусора (защита от падений mid-test и от
    тестов, не удаляющих свои объекты). Best-effort: возвращает список
    строк-ошибок, исключений не поднимает.

    :param module: экземпляр KnowledgeBase
    :param db_name: имя БД
    :param prefixes: префиксы имён тестового мусора (по умолчанию unittest_)
    :return: список строк-ошибок (пустой при полном успехе)
    """
    return clean_kb(module, db_name, prefixes)


def clean_kb(module, db_name, prefixes):
    """Удалить из KB объекты (контент, папки, наборы установки), имена
    которых начинаются с одного из `prefixes`.

    Best-effort: ошибки собираются в список и возвращаются, не поднимаются.
    Порядок: отвязка найденного контента от наборов (включая install-набор
    конвейера) -> удаление объектов -> удаление папок (глубина вниз) ->
    удаление наборов (глубина вниз).

    :param module: экземпляр KnowledgeBase
    :param db_name: имя БД
    :param prefixes: префиксы имён тестового мусора
    :return: список строк-ошибок (пустой при полном успехе)
    """
    errors = []

    def is_junk(obj_name):
        return any(str(obj_name or "").startswith(p) for p in prefixes)

    # 1. Контент (правила) - серверный поиск по префиксу, серверный фильтр
    # по типу быстрее полной выгрузки. search совпадения неточные - имя
    # проверяем на клиенте.
    junk_items = []
    junk_ids = set()
    for prefix in prefixes:
        for kind, siem_type in KB_CONTENT_KINDS.items():
            try:
                found = module.get_all_objects(
                    db_name,
                    {"search": prefix, "filters": {"SiemObjectType": [siem_type]}},
                )
                for obj in found:
                    if is_junk(obj.get("name")) and obj["id"] not in junk_ids:
                        junk_ids.add(obj["id"])
                        junk_items.append((obj["id"], kind, obj.get("name")))
            except Exception as err:
                errors.append(f"list content {siem_type} {prefix!r}: {err!r}")

    groups = module.get_groups_list(db_name, do_refresh=True)
    junk_group_ids = [
        gid
        for gid, g in groups.items()
        if is_junk(g.get("name")) and g.get("name") not in ("all", "install")
    ]

    # 2. Отвязать контент от удаляемых наборов и от install-набора конвейера
    remove_ids = list(junk_group_ids)
    try:
        remove_ids.append(module.get_install_deployment_set_id(db_name))
    except Exception as err:
        errors.append(f"install set: {err!r}")
    if junk_ids and remove_ids:
        try:
            module.link_content_to_groups(
                db_name, list(junk_ids), [], remove_group_ids=remove_ids
            )
        except Exception as err:
            errors.append(f"unlink: {err!r}")

    # 3. Удалить контент
    for obj_id, kind, name in junk_items:
        try:
            r = module.delete_content_item(db_name, obj_id, kind)
            if r.status_code != 204:
                errors.append(f"delete rule {name}: {r.status_code}")
        except Exception as err:
            errors.append(f"delete rule {name}: {err!r}")

    # 4. Папки - от вложенных к корню (непустая/родительская не удалится раньше)
    folders = module.get_folders_list(db_name, do_refresh=True)
    junk_folders = sorted(
        (fid for fid, f in folders.items() if is_junk(f.get("name"))),
        key=lambda fid: _depth(fid, folders),
        reverse=True,
    )
    for fid in junk_folders:
        try:
            r = module.delete_folder(db_name, fid)
            if r.status_code != 204:
                errors.append(f"delete folder {folders[fid]['name']}: {r.status_code}")
        except Exception as err:
            errors.append(f"delete folder {folders[fid]['name']}: {err!r}")

    # 5. Наборы установки - от вложенных к корню
    for gid in sorted(junk_group_ids, key=lambda g: _depth(g, groups), reverse=True):
        try:
            r = module.delete_group(db_name, gid)
            if r.status_code != 204:
                errors.append(f"delete group {groups[gid]['name']}: {r.status_code}")
        except Exception as err:
            errors.append(f"delete group {groups[gid]['name']}: {err!r}")

    return errors
