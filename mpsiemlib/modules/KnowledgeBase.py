import os
import time
from collections.abc import Iterator
from hashlib import sha256
from typing import Any, ClassVar, cast

import requests

from mpsiemlib.common import (
    AuthError,
    LoggingHandler,
    ModuleInterface,
    MPComponents,
    MPContentTypes,
    MPSIEMAuth,
    Settings,
    exec_request,
    get_metrics_start_time,
    get_metrics_took_time,
)

from .Conveyor import Conveyor


class KnowledgeBase(ModuleInterface, LoggingHandler):
    """PT KB module."""

    __kb_port = 8091

    #  обрабатывается в KB
    __api_root = "/api-studio"
    __api_kb_db_list = f"{__api_root}/content-database-selector/content-databases"
    __api_temp_file_storage_upload = f"{__api_root}/tempFileStorage/upload"

    __api_siem = f"{__api_root}/siem"
    __api_deploy_object = f"{__api_siem}/deploy"
    __api_deploy_log = f"{__api_siem}/deploy/log"
    __api_list_objects = f"{__api_siem}/objects/list"
    __api_table_info = f"{__api_siem}/tabular-lists"
    __api_folders_packs_list = f"{__api_siem}/folders/tree?includeObjects=false"
    __api_folders = f"{__api_siem}/folders"
    __api_co_rules = f"{__api_siem}/correlation-rules"
    __api_export = f"{__api_siem}/export"
    __api_import = f"{__api_siem}/import"
    __api_pipelines = f"{__api_siem}/pipelines"
    __api_compile = f"{__api_siem}/compile"
    __api_mass_operations = f"{__api_siem}/mass-operations"
    __api_siem_objgroups_values = f"{__api_mass_operations}/SiemObjectGroup/values"
    __api_mass_operations_move = f"{__api_mass_operations}/move"
    __api_mass_operations_delete = f"{__api_mass_operations}/delete"
    __api_mass_operations_check_move = f"{__api_mass_operations}/check-move"
    __api_mass_operations_check_delete = f"{__api_mass_operations}/check-delete"

    __api_siem_knowledge_packs = f"{__api_siem}/knowledge-packs"
    __api_siem_knowledge_packs_seen = f"{__api_siem_knowledge_packs}/seen"
    __api_siem_objects_filters = f"{__api_siem}/objects/filters"
    __api_siem_siem_settings_current = f"{__api_siem}/siem-settings/current"
    __api_siem_rules_compilation_statuses = f"{__api_siem}/rules-compilation-statuses"
    __api_compile_status = f"{__api_siem}/compile/status"

    __api_deployment_sets = f"{__api_siem}/deploymentSets"
    __api_deploy_stats = f"{__api_deploy_object}/stats"

    __api_databases_root = f"{__api_root}/databases"
    __api_databases_revisions = f"{__api_databases_root}/revisions"
    __api_content_databases = f"{__api_databases_root}/content-databases"
    __api_content_databases_current = f"{__api_content_databases}/current"
    __api_content_databases_tree_details = (
        f"{__api_content_databases_current}/tree-details"
    )
    __api_content_databases_merge_details = (
        f"{__api_content_databases_current}/merge-details"
    )
    __api_content_databases_tree_actions = (
        f"{__api_content_databases_current}/tree-actions"
    )
    __api_content_databases_is_updatable = (
        f"{__api_content_databases_current}/is-updatable"
    )
    __api_content_databases_is_deployable = (
        f"{__api_content_databases_current}/is-deployable"
    )
    __api_content_databases_pull_start = f"{__api_content_databases_current}/pull/start"
    __api_content_databases_pull_stop = f"{__api_content_databases_current}/pull/stop"
    __api_content_databases_parent_top_revision = (
        f"{__api_content_databases}/parent/revisions/top"
    )

    __api_content_distribution_applications = (
        f"{__api_root}/content-distribution/applications"
    )
    __api_migration_info = f"{__api_root}/migration/migrationInfo"

    # Контрактный Content API (CoreApi.*.yaml), обслуживается на KB-стороне
    __api_content_root = "/api"
    __api_contract_export = f"{__api_content_root}/siem/export"
    __api_content_origins = f"{__api_content_root}/Origins"
    __api_content_rule_classes = f"{__api_content_root}/RuleClasses"
    __api_content_rule_localizations = f"{__api_content_root}/RuleLocalizations"
    __api_content_siem_settings_info = f"{__api_content_root}/SiemSettings/info"

    # обрабатывается в Core
    __api_rules_v2 = "/api/siem/v2/rules"

    # Форматы экспорта
    EXPORT_FORMAT_KB = "kb"
    EXPORT_FORMAT_SIEM_LITE = "siem"

    # Режимы импорта

    # Добавить и обновить объекты из файла
    #
    # Все объекты из файла добавятся как пользовательские.
    # Существующие в системе объекты будут заменены, в том числе
    # записи табличных списков.
    IMPORT_ADD_AND_UPDATE = "upsert"

    # Добавить объекты Локальная система как системные
    #
    # Будут импортированы только объекты Локальная система.
    # Новые объекты добавятся, существующие будут заменены.
    IMPORT_LOCAL_SYSTEM_AS_SYSTEM = "upsert_origin"

    # Синхронизировать объекты Локальная система с содержимым файла
    #
    # Будут импортированы только объекты Локальная система.
    # Существующие объекты будут заменены на объекты из файла,
    # а объекты, которых нет в файле, будут удалены из системы.
    IMPORT_SYNC_SYSTEM = "replace_origin"

    # Маппинг типа контента в часть url для запросов
    ITEM_TYPE_MAP: ClassVar[dict[str, str]] = {
        "CorrelationRule": "correlation-rules",
        "AggregationRule": "aggregation-rules",
        "EnrichmentRule": "enrichment-rules",
        "NormalizationRule": "normalization-rules",
        "TabularList": "tabular-lists",
    }

    # Статусы установки в SIEM для контента
    DEPLOYMENT_STATUS_INSTALLED = "Installed"
    DEPLOYMENT_STATUS_NOT_INSTALLED = "NotInstalled"

    # Статусы компиляции объекта (CompilationStatus.CompilationStatusId)
    COMPILATION_STATUS_SUCCESS = 1
    COMPILATION_STATUS_ERROR = 2
    COMPILATION_STATUS_NEEDED = 3

    # Корень дерева наборов установки (RuleClass 'all' / 'Все объекты').
    # Наборы, созданные вне этого узла, UI KB не показывает.
    GROUPS_ROOT_ID = "00000000-0000-0000-0000-000000000001"

    # Имя набора установки конвейера (RuleClass, состав которого ставит
    # `deploy`). Создается стендом, см. `get_install_deployment_set_id`.
    INSTALL_SET_NAME = "install"

    # Базовые пути контрактного Content API по типам контента
    # (CoreApi.{Correlation,Aggregation,Enrichment,Normalization}Rules.yaml)
    CONTENT_RULE_API_MAP: ClassVar[dict[str, str]] = {
        MPContentTypes.CORRELATION: "/api/CorrelationRules",
        MPContentTypes.AGGREGATION: "/api/AggregationRules",
        MPContentTypes.ENRICHMENT: "/api/EnrichmentRules",
        MPContentTypes.NORMALIZATION: "/api/NormalizationRules",
    }

    # Суффиксы kb-ui путей правил по типу контента (`/{type}-rules`)
    CONTENT_RULE_PATH_MAP: ClassVar[dict[str, str]] = {
        MPContentTypes.CORRELATION: "correlation-rules",
        MPContentTypes.AGGREGATION: "aggregation-rules",
        MPContentTypes.ENRICHMENT: "enrichment-rules",
        MPContentTypes.NORMALIZATION: "normalization-rules",
        MPContentTypes.TABLE: "tabular-lists",
    }

    # Буквенные коды типов объектов в коротком ObjectId (kb-ui letterTypeMap,
    # ответ `GET /api-studio/siem/search/objectId/{shortId}`)
    SHORT_ID_TYPE_MAP: ClassVar[dict[str, str]] = {
        "X": "NormalizationRule",
        "A": "AggregationRule",
        "C": "CorrelationRule",
        "E": "EnrichmentRule",
        "T": "TabularList",
        "M": "Macro",
    }

    # Режимы массовых операций (mass-operations check-*/move/delete)
    MASS_OPERATION_MODE_SELECTION = "selection"
    MASS_OPERATION_MODE_ALL = "all"
    MASS_OPERATION_MODE_DEPLOYMENT_SET = "deploymentSet"
    # Режимы выделения в selectedObjects.selectionMode
    SELECTION_MODE_SELECTED = "Selected"
    SELECTION_MODE_EXCLUDED = "Excluded"

    # Тайм-аут операций установки контента из SIEM. В 26.0 установка - полная
    # переустановка pending-контента и занимает десятки минут.
    DEPLOYMENT_TIMEOUT = 15
    # Количество проверок статуса установки (DEPLOYMENT_TIMEOUT * DEPLOYMENT_RETRIES)
    DEPLOYMENT_RETRIES = 200

    # Тайм-аут и попытки ожидания компиляции объекта
    COMPILATION_TIMEOUT = 5
    COMPILATION_RETRIES = 30

    def __init__(self, auth: MPSIEMAuth, settings: Settings) -> None:
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        sessions = auth.sessions or {}
        kb_session = sessions.get("kb")
        if kb_session is None:
            # MPSIEMWorker подключает только core/ms, поэтому KB-сессию
            # поднимаем лениво при первом обращении к модулю
            kb_session = auth.connect(MPComponents.KB)
        self.__kb_session = kb_session
        creds = auth.get_creds()
        if creds is None or creds.core_hostname is None:
            raise AuthError("Core hostname is not set")
        self.__kb_hostname = creds.core_hostname
        # Разрешение siem_id для RulesAPI (Core `/api/siem/v2/rules`):
        # при одном конвейере параметр не передаётся, при нескольких -
        # обязателен (см. Conveyor.resolve_siem_id)
        self.__conveyor = Conveyor(auth, settings)
        self.__rules_mapping: dict[str, dict[str, Any]] = {}
        self.__groups: dict[str, dict[str, Any]] = {}
        self.__folders: dict[str, dict[str, Any]] = {}
        self.__packs: dict[str, dict[str, Any]] = {}
        self.log.debug('status=success, action=prepare, msg="KB Module init"')

    def get_pipelines_list(self, db_name: str | None = None) -> list[dict[str, Any]]:
        """Список SIEM-конвейеров, доступных для установки контента.

        MP SIEM 26.0: установка контента выполняется push-моделью на
        конвейер(ы). Контракт KB-стороны `GET /api-studio/siem/pipelines`
        (не путать с id конвейеров в `siem_manager`). Идентификатор конвейера
        здесь - это `siemObjectUid`, который ожидает `deploy`.

        :param db_name: Имя БД (None - первая deployable)
        :return: [{'id', 'url', 'alias', 'version'}]
        """
        db_name = self.__resolve_db_name(db_name)
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_pipelines}"

        r = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            headers=headers,
        )
        pipelines: list[dict[str, Any]] = [
            {
                "id": p.get("Id"),
                "url": p.get("Url"),
                "alias": p.get("Alias"),
                "version": p.get("Version"),
            }
            for p in r.json()
        ]

        self.log.info(
            f'status=success, action=get_pipelines_list, msg="Got {len(pipelines)} pipelines", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )
        return pipelines

    def compile_objects(
        self, db_name: str | None, guids_list: list[str]
    ) -> requests.Response:
        """Скомпилировать выбранные объекты контента.

        MP SIEM 26.0: только скомпилированный объект попадает в pending-состав
        конвейера и может быть установлен (``GeneralDeploymentStatus`` меняется
        с ``NotInstalled`` на ``Installed`` только после compile + deploy).
        Контракт KB-стороны ``POST /api-studio/siem/compile``:
        ``{"mode": "selection", "appliesTo": "All",
        "selectedObjects": {...}, "filter": {...}}``.

        :param db_name: Имя БД (None - первая deployable)
        :param guids_list: список ID объектов контента
        :return: Объект Response (201 - компиляция запущена)
        """
        db_name = self.__resolve_db_name(db_name)
        self.log.info(
            f'status=prepare, action=compile_objects, msg="Try to compile objects '
            f'{guids_list}", hostname="{self.__kb_hostname}", db="{db_name}"'
        )
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        params: dict[str, Any] = {
            "mode": "selection",
            "appliesTo": "All",
            "selectedObjects": {"ids": guids_list, "selectionMode": "Selected"},
            "filter": {
                "filters": None,
                "search": "",
                "folderId": None,
                "groupId": None,
                "recursive": False,
            },
        }

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_compile}"
        return exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )

    def compile_objects_sync(
        self,
        db_name: str | None,
        content_type: str,
        guids_list: list[str],
        timeout: int = COMPILATION_TIMEOUT,
        max_retries: int = COMPILATION_RETRIES,
    ) -> None:
        """Скомпилировать объекты и дождаться успешной компиляции.

        Статус компиляции объекта крутится в ``CompilationStatus.CompilationStatusId``
        (``1`` - success, ``2`` - error, ``3`` - needs compilation).

        :param db_name: имя БД (None - первая deployable)
        :param content_type: тип объектов MPContentTypes
        :param guids_list: список ID объектов контента
        :param timeout: тайм-аут между попытками проверки статуса компиляции
        :param max_retries: максимальное количество попыток
        :raises RuntimeError: если компиляция завершилась с ошибкой
        :raises TimeoutError: если компиляция не завершилась за отведённое время
        """
        db_name = self.__resolve_db_name(db_name)
        self.compile_objects(db_name, guids_list)

        remaining = set(guids_list)
        for _ in range(max_retries):
            time.sleep(timeout)
            mapping: dict[str, dict[str, Any]] = {}
            for obj in self.get_all_objects(
                db_name, {"filters": {"SiemObjectType": [content_type]}}
            ):
                mapping[obj["id"]] = obj
            for guid in list(remaining):
                cached: dict[str, Any] | None = mapping.get(guid)
                status = (cached or {}).get("compilation_status")
                if status == self.COMPILATION_STATUS_SUCCESS:
                    remaining.discard(guid)
                elif status == self.COMPILATION_STATUS_ERROR:
                    raise RuntimeError(f"Compilation failed for {content_type} {guid}")
            if not remaining:
                self.log.info(
                    f"status=success, action=compile_objects_sync, "
                    f'msg="Objects compiled", hostname="{self.__kb_hostname}", '
                    f'db="{db_name}", objects={len(guids_list)}'
                )
                return

        raise TimeoutError(
            f"Compilation did not finish in {max_retries} retries: {sorted(remaining)}"
        )

    def deploy(
        self, db_name: str | None = None, pipeline_uids: list[str] | None = None
    ) -> list[str]:
        """Установить pending-контент в SIEM push-моделью на конвейеры.

        MP SIEM 26.0: ``POST /api-studio/siem/deploy`` принимает в теле
        массив ``siemObjectUid`` конвейеров (``get_pipelines_list``) и
        устанавливает **весь** pending-контент БД (NotInstalled/Outdated) на
        указанные конвейеры. Установка конкретного объекта (или набора) этим
        эндпоинтом не выполняется - объект попадает в pending после
        ``compile_objects``. Ответ содержит по одному deployment на конвейер.

        :param db_name: Имя БД (None - первая deployable)
        :param pipeline_uids: идентификаторы конвейеров; по умолчанию - все
            конвейеры из ``get_pipelines_list``
        :return: список deploy ID (по одному на конвейер)
        :raises ValueError: если конвейеры не найдены
        """
        db_name = self.__resolve_db_name(db_name)
        if pipeline_uids is None:
            pipeline_uids = [p["id"] for p in self.get_pipelines_list(db_name)]

        if not pipeline_uids:
            raise ValueError(f"No pipelines to deploy to in db {db_name!r}")

        self.log.info(
            f'status=prepare, action=deploy, msg="Try to deploy pending content '
            f'to pipelines {pipeline_uids}", hostname="{self.__kb_hostname}", '
            f'db="{db_name}"'
        )
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_deploy_object}"
        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=list(pipeline_uids),
        )
        response: list[dict[str, Any]] = r.json()

        deploy_ids = [str(deployment["Id"]) for deployment in response]
        self.log.info(
            f'status=success, action=deploy, msg="Deploy started", '
            f'hostname="{self.__kb_hostname}", db="{db_name}", '
            f"deploy_ids={deploy_ids}"
        )
        return deploy_ids

    def deploy_sync(
        self,
        db_name: str | None = None,
        pipeline_uids: list[str] | None = None,
        timeout: int = DEPLOYMENT_TIMEOUT,
        max_retries: int = DEPLOYMENT_RETRIES,
    ) -> None:
        """Запустить установку pending-контента и дождаться её завершения.

        Установка в 26.0 - полная переустановка pending-контента на конвейер и
        занимает значительное время (десятки минут на большой БД), поэтому
        ``max_retries`` завышен.

        :param db_name: имя БД (None - первая deployable)
        :param pipeline_uids: идентификаторы конвейеров (см. ``deploy``)
        :param timeout: тайм-аут между попытками проверки статуса установки
        :param max_retries: максимальное количество попыток
        :raises RuntimeError: если установка завершилась с ошибкой
        :raises TimeoutError: если установка не завершилась за отведённое время
        """
        db_name = self.__resolve_db_name(db_name)
        deploy_ids = self.deploy(db_name, pipeline_uids)

        remaining = set(deploy_ids)
        for _ in range(max_retries):
            time.sleep(timeout)
            for deploy_id in list(remaining):
                status = self.get_deploy_status(db_name, deploy_id)
                deployment_status = status["deployment_status"]
                if deployment_status == "succeeded":
                    remaining.discard(deploy_id)
                elif deployment_status == "running":
                    continue
                else:
                    raise RuntimeError(
                        f"Deploy {deploy_id} finished with status {deployment_status!r}"
                    )
            if not remaining:
                self.log.info(
                    f'status=success, action=deploy_sync, msg="Deploy succeed", '
                    f'hostname="{self.__kb_hostname}", db="{db_name}"'
                )
                return

        raise TimeoutError(
            f"Deploy did not finish in {max_retries} retries: {sorted(remaining)}"
        )

    def get_install_deployment_set_id(self, db_name: str | None = None) -> str:
        """ID набора установки конвейера (``RuleClass`` с именем 'install').

        MP SIEM 26.0: ``deploy`` устанавливает на конвейер состав его
        install-набора, а не произвольно выбранные объекты. Чтобы объект был
        установлен, он должен быть привязан к этому набору
        (``link_content_to_groups``/``bulk-add`` не работают, см.
        ``install_content``).

        :param db_name: Имя БД (None - первая deployable)
        :return: ID install-набора
        :raises ValueError: если набор с SystemName 'install' не найден
        """
        db_name = self.__resolve_db_name(db_name)

        for group_id, group_data in self.get_groups_list(
            db_name, do_refresh=True
        ).items():
            if group_data.get("name") == self.INSTALL_SET_NAME:
                return cast(str, group_id)

        raise ValueError(f"Deployment set {self.INSTALL_SET_NAME!r} not found")

    def install_content(
        self,
        db_name: str | None,
        content_type: str,
        guids_list: list[str],
        pipeline_uids: list[str] | None = None,
        deployment_set_id: str | None = None,
        compile_timeout: int = COMPILATION_TIMEOUT,
        compile_retries: int = COMPILATION_RETRIES,
        deploy_timeout: int = DEPLOYMENT_TIMEOUT,
        deploy_retries: int = DEPLOYMENT_RETRIES,
    ) -> None:
        """Полный цикл установки контента в 26.0: link -> compile -> deploy.

        Эквивалент прежнего ``install_objects_sync``: в 26.0 ``deploy``
        устанавливает на конвейер состав install-набора (``RuleClass``
        ``install``, см. ``get_install_deployment_set_id``), поэтому объект
        сначала привязывается к этому набору (``link_content_to_groups``),
        затем компилируется (``compile_objects_sync``) и только потом
        развёртывается (``deploy_sync``). Без привязки deploy завершится
        успешно, но объект останется ``NotInstalled``.

        :param db_name: имя БД (None - первая deployable)
        :param content_type: тип объектов MPContentTypes
        :param guids_list: список ID объектов контента
        :param pipeline_uids: идентификаторы конвейеров (см. ``deploy``)
        :param deployment_set_id: набор установки, к которому привязывать
            объекты; по умолчанию - install-набор (``install``). None не
            поддерживается - без привязки установка бессмысленна.
        :param compile_timeout: тайм-аут ожидания компиляции
        :param compile_retries: попытки ожидания компиляции
        :param deploy_timeout: тайм-аут ожидания установки
        :param deploy_retries: попытки ожидания установки
        """
        db_name = self.__resolve_db_name(db_name)
        if deployment_set_id is None:
            deployment_set_id = self.get_install_deployment_set_id(db_name)

        self.link_content_to_groups(db_name, guids_list, [deployment_set_id])
        self.compile_objects_sync(
            db_name, content_type, guids_list, compile_timeout, compile_retries
        )
        self.deploy_sync(db_name, pipeline_uids, deploy_timeout, deploy_retries)

    def get_deploy_status(self, db_name: str | None, deploy_id: str) -> dict[str, str]:
        """Получить статус процесса установки контента.

        MP SIEM 26.0: пер-deployment эндпоинта нет; ``POST
        /api-studio/siem/deploy/log`` (фильтр-тело игнорируется) возвращает
        полный журнал, из которого нужный deployment ищется по ``Id``. Поля
        ``Percentage``/``Errors`` в 26.0 отсутствуют - возвращаются пустыми.

        :param db_name: Имя БД (None - первая deployable)
        :param deploy_id: Идентификатор процесса установки
        :return: {"deployment_status": "succeeded|running|failed",
            "start_date": ..., "pipeline_id": ..., "percentage": "", "errors": ""}
        """
        db_name = self.__resolve_db_name(db_name)
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_deploy_log}"
        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json={},
        )
        entries: list[dict[str, Any]] = r.json()

        state: dict[str, Any] = next(
            (e for e in entries if e.get("Id") == deploy_id), {}
        )

        deployment_status = str(state.get("DeployStatusId", ""))

        self.log.info(
            f'status=success, action=get_deploy_status, msg="Deploy status: '
            f'{deployment_status}", hostname="{self.__kb_hostname}", '
            f'db="{db_name}", deploy_id="{deploy_id}"'
        )
        return {
            "deployment_status": deployment_status,
            "start_date": str(state.get("StartDate", "")),
            "pipeline_id": str(state.get("PipelineId", "")),
            "percentage": "",
            "errors": "",
        }

    def start_rule(
        self,
        db_name: str | None,
        content_type: str,
        guids_list: list,
        siem_id: str | None = None,
    ) -> None:
        """Запустить правила, установленные в SIEM Server Используются ID
        правил из KB.

        :param db_name: Имя БД в KB (None - первая deployable)
        :param content_type: MPContentType
        :param guids_list: Список ID правил для установки
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return:
        """
        db_name = self.__resolve_db_name(db_name)
        self.__manipulate_rule(db_name, content_type, guids_list, "start", siem_id)

    def stop_rule(
        self,
        db_name: str | None,
        content_type: str,
        guids_list: list,
        siem_id: str | None = None,
    ) -> None:
        """Остановить правило, установленное в SIEM Server Используются ID
        правил из KB.

        :param db_name: Имя БД в KB (None - первая deployable)
        :param content_type: MPContentType
        :param guids_list: Список ID правил для установки
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return:
        """
        db_name = self.__resolve_db_name(db_name)
        self.__manipulate_rule(db_name, content_type, guids_list, "stop", siem_id)

    def set_default_conveyor(self, siem_id: str | None = None) -> str:
        """Задать конвейер по умолчанию для операций RulesAPI (start/stop,
        состояние правила).

        Нужно, когда в Core зарегистрировано несколько конвейеров: RulesAPI
        без ``siem_id`` не понимает, к какому SIEM обращаться. На ``deploy``
        не влияет - там конвейер выбирается через ``pipelines`` KB.

        :param siem_id: id конвейера (GUID); None — основной конвейер
        :return: Установленный siem_id
        """
        return self.__conveyor.set_default_conveyor(siem_id)

    def __rules_api_params(self, siem_id: str | None) -> dict[str, str]:
        """Query-параметры RulesAPI (``siem_id`` конвейера).

        Пусто, если конвейер в Core один. Явный ``siem_id`` приоритетнее
        default; при нескольких конвейерах без выбора — ``ValueError``.
        """
        resolved = self.__conveyor.resolve_siem_id(siem_id)
        return {} if resolved is None else {"siem_id": resolved}

    @staticmethod
    def __rule_object_type(content_type: str) -> str:
        """SIEM-часть URL для типа контента правила."""
        if content_type == MPContentTypes.CORRELATION:
            return "correlation"
        if content_type == MPContentTypes.ENRICHMENT:
            return "enrichment"
        raise ValueError(f"Unsupported content type for rule control: {content_type}")

    def __rules_mapping_populated(self, db_name: str, content_type: str) -> bool:
        """Есть ли в кэше маппинг id->имя для конкретной пары (БД, тип).

        Кэш заводится отдельно на каждую пару: признак заполненности - наличие
        ключа ``content_type`` (``__update_rules_mapping`` заводит его даже для
        пустого типа), а не размер всего кэша. Иначе первый же закэшированный
        тип блокировал заполнение остальных.
        """
        return content_type in self.__rules_mapping.get(db_name, {})

    def __rule_name_by_id(self, db_name: str, content_type: str, guid: str) -> str:
        """Имя правила в SIEM по его ID из KB."""
        if not self.__rules_mapping_populated(db_name, content_type):
            self.__update_rules_mapping(db_name, content_type)

        name = (
            self.__rules_mapping.get(db_name, {})
            .get(content_type, {})
            .get(guid, {})
            .get("name")
        )
        if name is None:
            # Недавно созданное правило может отсутствовать в кэше - один раз
            # пересчитываем маппинг этого типа и ищем снова.
            self.__update_rules_mapping(db_name, content_type)
            name = (
                self.__rules_mapping.get(db_name, {})
                .get(content_type, {})
                .get(guid, {})
                .get("name")
            )
        if name is None:
            self.log.error(
                'status=failed, action=rule_name_by_id, msg="Rule id not found", '
                f'hostname="{self.__kb_hostname}", db="{db_name}", rule_id="{guid}"'
            )
            raise ValueError(f"Rule id not found in KB: {guid}")
        return str(name)

    def __manipulate_rule(
        self,
        db_name: str,
        content_type: str,
        guids_list: list,
        control: str = "stop",
        siem_id: str | None = None,
    ) -> None:
        # Нет гарантий, что объекты в PT KB и SIEM будут называться одинаково.
        # Сейчас в классе прописаны названия из PT KB. Название табличек уже разное.
        object_type = self.__rule_object_type(content_type)
        rules_names = [
            self.__rule_name_by_id(db_name, content_type, guid) for guid in guids_list
        ]

        data = {"names": rules_names}
        api_url = f"{self.__api_rules_v2}/{object_type}/commands/{control}"
        url = f"https://{self.__kb_hostname}{api_url}"
        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            params=self.__rules_api_params(siem_id),
            json=data,
        )
        response: dict[str, Any] = r.json()

        errors = response.get("error") or []
        if len(errors) != 0:
            self.log.error(
                "status=failed, action=manipulate_rule, "
                f'msg="Got error while manipulate rule", hostname="{self.__kb_hostname}", '
                f'rules_ids="{guids_list}", rules_names="{rules_names}", '
                f'db="{db_name}", error="{errors}"'
            )
            raise RuntimeError(f"Got error while manipulate rule: {errors}")

        self.log.info(
            f'status=success, action=manipulate_rule, msg="{control} {object_type} rules", '
            f'hostname="{self.__kb_hostname}", rules_names="{rules_names}", db="{db_name}"'
        )

    def get_rule_running_state(
        self,
        db_name: str | None,
        content_type: str,
        guid: str,
        siem_id: str | None = None,
    ) -> dict[str, Any]:
        """Получить статус правила, работающего в SIEM Server. Используются ID
        правил из KB.

        :param db_name: Имя БД в KB (None - первая deployable)
        :param content_type: MPContentType
        :param guid: ID правила
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return: {"state": ..., "reason": ..., "context": ...}
        """
        db_name = self.__resolve_db_name(db_name)
        object_type = self.__rule_object_type(content_type)
        name = self.__rule_name_by_id(db_name, content_type, guid)

        api_url = f"{self.__api_rules_v2}/{object_type}/{name}"
        url = f"https://{self.__kb_hostname}{api_url}"
        r = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=self.__rules_api_params(siem_id),
        )
        response: dict[str, Any] = r.json()

        state: dict[str, Any] = response.get("state", {})

        return {
            "state": state.get("name"),
            "reason": state.get("reason"),
            "context": state.get("context"),
        }

    def get_databases_list(self) -> dict[str, dict[str, Any]]:
        """Получить список БД.

        :return: {'db_name': {'param1': 'value1'}}
        """
        # TODO Не учитывается что БД с разными родительскими БД могут иметь одинаковое имя
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_kb_db_list}"
        r = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        db_names: list[dict[str, Any]] = r.json()
        ret: dict[str, dict[str, Any]] = {}
        for i in db_names:
            name = i.get("Name")
            if name is None:
                continue
            status = i.get("Status")
            ret[str(name)] = {
                "id": i.get("Uid"),
                "status": status.lower() if isinstance(status, str) else None,
                "updatable": i.get("IsUpdatable"),
                "deployable": i.get("IsDeployable"),
                "parent": i.get("ParentName"),
                "revisions": i.get("RevisionsCount"),
            }

        self.log.info(
            f'status=success, action=get_databases_list, msg="Got {len(ret)} databases", '
            f'hostname="{self.__kb_hostname}"'
        )

        return ret

    def get_deployable_db_name(self) -> str:
        """Получить первую deployable БД из ``get_databases_list``.

        :return: Имя первой БД с ``deployable=True``
        :raises ValueError: если deployable БД на стенде нет
        """
        for name, params in self.get_databases_list().items():
            if params.get("deployable"):
                return name

        raise ValueError("No deployable content database (deployable=true) on stand")

    def __resolve_db_name(self, db_name: str | None) -> str:
        """Использовать первую deployable БД, если ``db_name`` не указан."""
        if db_name is not None:
            return db_name

        return self.get_deployable_db_name()

    def get_groups_list(
        self, db_name: str | None = None, do_refresh: bool = False
    ) -> dict:
        """Получить список наборов установки.

        В MP SIEM 26.0 «наборы для установки» реализованы как классы правил
        (`RuleClasses`): прежний эндпоинт `GET /api-studio/siem/groups` удалён
        (404). Миграция: контракт `CoreApi.RuleClasses.yaml`
        `GET /api/RuleClasses/` (GetRuleClasses) c
        `consideringParentId=false` - плоский список всех узлов дерева.

        :param db_name: Имя БД (None - первая deployable)
        :param do_refresh: Обновить кэш
        :return: {'group_id': {'parent_id': 'value', 'name': 'value'}}
        """
        db_name = self.__resolve_db_name(db_name)
        if not do_refresh and len(self.__groups) != 0:
            return self.__groups

        groups = self.__content_api_request(
            db_name,
            self.__api_content_rule_classes,
            "get_groups_list",
            params={"consideringParentId": "false"},
        )
        self.__groups.clear()

        for i in groups:
            self.__groups[i.get("Id")] = {
                "parent_id": i.get("ParentRuleClassId"),
                "name": i.get("SystemName"),
            }

        self.log.info(
            f'status=success, action=get_groups_list, msg="Got {len(self.__groups)} groups", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

        return self.__groups

    def get_folders_list(
        self, db_name: str | None = None, do_refresh: bool = False
    ) -> dict:
        """Получить список папок.

        :param db_name: Имя БД (None - первая deployable)
        :param do_refresh: Обновить кэш
        :return: {'group_id': {'parent_id': 'value', 'name': 'value'}}
        """
        db_name = self.__resolve_db_name(db_name)
        if do_refresh or len(self.__folders) == 0:
            self.__folders.clear()
            self.__packs.clear()
            self.__get_folder_pack_root_level(db_name)

            self.log.info(
                f'status=success, action=get_folders_list, msg="Got {len(self.__folders)} folders", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return self.__folders

    def get_packs_list(
        self, db_name: str | None = None, do_refresh: bool = False
    ) -> dict:
        """Получить список паков.

        :param db_name: Имя БД (None - первая deployable)
        :param do_refresh: Обновить кэш
        :return: {'group_id': {'parent_id': 'value', 'name': 'value'}}
        """
        db_name = self.__resolve_db_name(db_name)
        if do_refresh or len(self.__packs) == 0:
            self.__get_folder_pack_root_level(db_name)

            self.log.info(
                f'status=success, action=get_packs_list, msg="Got {len(self.__packs)} packs", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return self.__packs

    def __iterate_folders_tree(self, db_name: str, folder_id: str) -> None:
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        folder_url = f"{self.__api_folders}/{folder_id}/children?includeObjects=false"

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{folder_url}"

        r = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            headers=headers,
        )

        self.__collect_folder_pack_nodes(r.json(), db_name)

    def __collect_folder_pack_nodes(
        self, folders_packs: list[dict[str, Any]], db_name: str
    ) -> None:
        """Разложить узлы дерева по кэшам папок/паков и рекурсивно обойти
        вложенные папки."""
        for node in folders_packs:
            node_type = node.get("NodeKind")
            if node_type == "Folder":
                current = self.__folders
            elif node_type == "KnowledgePack":
                current = self.__packs
            else:
                continue

            obj_id = node.get("Id")
            if obj_id is None:
                continue
            current[str(obj_id)] = {
                "parent_id": node.get("ParentId"),
                "name": node.get("Name"),
            }

            if node_type == "Folder" and node.get("HasChildren"):
                self.__iterate_folders_tree(db_name, str(obj_id))

    def __get_folder_pack_root_level(self, db_name: str) -> None:
        params: dict[str, Any] = {"expandNodes": []}
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_folders_packs_list}"

        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )

        self.__collect_folder_pack_nodes(r.json(), db_name)

    def get_normalizations_list(
        self, db_name: str | None = None, filters: dict | None = None
    ) -> Iterator[dict]:
        """Получить список правил нормализации.

        :param db_name: Имя БД (None - первая deployable)
        :param filters: см get_all_objects
        :return: Iterator
        """
        db_name = self.__resolve_db_name(db_name)
        params = {"filters": {"SiemObjectType": ["Normalization"]}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_correlations_list(
        self, db_name: str | None = None, filters: dict | None = None
    ) -> Iterator[dict]:
        """Получить список правил корреляции.

        :param db_name: Имя БД (None - первая deployable)
        :param filters: см get_all_objects
        :return: Iterator
        """
        db_name = self.__resolve_db_name(db_name)
        params = {"filters": {"SiemObjectType": ["Correlation"]}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_enrichments_list(
        self, db_name: str | None = None, filters: dict | None = None
    ) -> Iterator[dict]:
        """Получить список правил обогащения.

        :param db_name: Имя БД (None - первая deployable)
        :param filters: см get_all_objects
        :return: Iterator
        """
        db_name = self.__resolve_db_name(db_name)
        params = {"filters": {"SiemObjectType": ["Enrichment"]}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_aggregations_list(
        self, db_name: str | None = None, filters: dict | None = None
    ) -> Iterator[dict]:
        """Получить список правил агрегации.

        :param db_name: Имя БД (None - первая deployable)
        :param filters: см get_all_objects
        :return: Iterator
        """
        db_name = self.__resolve_db_name(db_name)
        params = {"filters": {"SiemObjectType": ["Aggregation"]}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_tables_list(
        self, db_name: str | None = None, filters: dict | None = None
    ) -> Iterator[dict]:
        """Получить список табличек.

        :param db_name: Имя БД (None - первая deployable)
        :param filters: см get_all_objects
        :return: Iterator
        """
        db_name = self.__resolve_db_name(db_name)
        params = {"filters": {"SiemObjectType": ["TabularList"]}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_all_objects(
        self,
        db_name: str | None = None,
        filters: dict | None = None,
        group_id: str | None = None,
    ) -> Iterator[dict]:
        """Выгрузка всех объектов, кроме макросов.

        :param db_name: Имя БД из которой идет выгрузка (None - первая deployable)
        :param filters: {"folderId": null, "filters": {
            "SiemObjectType": ["Normalization"], "ContentType":
            ["System"], "DeploymentStatus": ["0"], "CompilationStatus":
            ["2"], "SiemObjectRegex": [".*_test_name"] }, "search": "",
            "sort": [{"name": "objectId", "order": 0, "type": 1}],
            "groupId": null, }
        :param group_id: Идентификатор набора установки
        :return: {"param1": "value1", "param2": "value2"}
        """
        db_name = self.__resolve_db_name(db_name)
        self.log.info(
            'status=prepare, action=get_all_objects, msg="Try to get objects list", '
            f'hostname="{self.__kb_hostname}", db="{db_name}", filters="{filters}"'
        )

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_list_objects}"
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        params: dict[str, Any] = {
            "sort": [{"name": "objectId", "order": 0, "type": 1}],
        }
        if filters is not None:
            params.update(filters)

        if group_id:
            params["groupId"] = group_id

        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.kb_objects_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__iterate_objects(url, params, headers, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            for i in ret:
                line_counter += 1
                yield {
                    "id": i.get("Id"),
                    "guid": i.get("ObjectId"),
                    "name": i.get("SystemName"),
                    "folder_id": i.get("FolderId"),
                    "object_kind": i.get("ObjectKind"),
                    "folder_path": str(i.get("FolderPath")).replace("\\", "/")
                    if i.get("FolderPath")
                    else "",
                    "origin_id": i.get("OriginId"),
                    "compilation_sdk": (i.get("CompilationStatus") or {}).get(
                        "SdkVersion"
                    ),
                    "compilation_status": (i.get("CompilationStatus") or {}).get(
                        "CompilationStatusId"
                    ),
                    "deployment_status": str(i.get("DeploymentStatus") or "").lower(),
                }
        took_time = get_metrics_took_time(start_time)

        self.log.info(
            'status=success, action=get_all_objects, msg="Query executed, response have been read", '
            f'hostname="{self.__kb_hostname}", filter="{filters}", lines={line_counter}, db="{db_name}"'
        )
        self.log.info(
            f'hostname="{self.__kb_hostname}", metric=get_all_objects, took={took_time}ms, objects={line_counter}'
        )

    def __iterate_objects(
        self,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        offset: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        params["withoutGroups"] = False
        params["recursive"] = True
        params["skip"] = offset
        params["take"] = limit
        rq = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )
        response: dict[str, Any] | None = rq.json()
        if response is None or "Rows" not in response:
            self.log.error(
                'status=failed, action=kb_objects_iterate, msg="KB data request return None or '
                'has wrong response structure", '
                f'hostname="{self.__kb_hostname}"'
            )
            raise RuntimeError(
                "KB data request return None or has wrong response structure"
            )

        return list(response["Rows"])

    def get_id_by_name(
        self, db_name: str | None, content_type: str, object_name: str
    ) -> list[dict[str, Any]]:
        """Узнать ID объекта по его имени. KB позволяет создавать объекты с
        неуникальным именем.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentType
        :param object_name: Имя искомого объекта
        :return: [{'id': value, 'folder_id': value}]
        """
        db_name = self.__resolve_db_name(db_name)
        if not self.__rules_mapping_populated(db_name, content_type):
            self.__update_rules_mapping(db_name, content_type)

        ret: list[dict[str, Any]] = []
        for k, v in self.__rules_mapping[db_name][content_type].items():
            if v.get("name") == object_name:
                ret.append(
                    {"id": k, "folder_id": v.get("folder_id"), "guid": v.get("guid")}
                )
        return ret

    def __update_rules_mapping(self, db_name: str, content_type: str) -> None:
        """(Пере)собрать кэш маппинга id->имя по объектам одного типа.

        Кэш индексируется парой (БД, тип контента) и пересобирается целиком, чтобы
        повторный вызов актуализировал состав (например, после создания
        правила), а не дополнял устаревший.
        """
        db_mapping = self.__rules_mapping.setdefault(db_name, {})
        type_mapping: dict[str, dict[str, Any]] = {}
        params: dict[str, Any] = {"filters": {"SiemObjectType": [content_type]}}
        for i in self.get_all_objects(db_name, params):
            type_mapping[i["id"]] = {
                "name": i.get("name"),
                "folder_id": i.get("folder_id"),
                "guid": i.get("guid"),
            }
        db_mapping[content_type] = type_mapping

    def get_rule(self, db_name: str | None, content_type: str, rule_id: str) -> dict:
        """Получить полное описание и тело правила.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentType
        :param rule_id: KB ID правила
        :return: {'param1': value, 'param2': value}
        """
        db_name = self.__resolve_db_name(db_name)
        if content_type == MPContentTypes.TABLE:
            raise NotImplementedError(
                f"Method get_rule not supported for {MPContentTypes.TABLE}"
            )

        self.log.info(
            f'status=success, action=get_rule, msg="Try to get rule {rule_id}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        api_url = f"{self.__api_siem}/{content_type.lower()}-rules/{rule_id}"
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{api_url}"

        r = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            headers=headers,
        )
        rule: dict[str, Any] = r.json()

        rule_groups = [
            {"id": i.get("Id"), "name": i.get("SystemName")}
            for i in rule.get("Groups") or []
        ]

        compilation_status = rule.get("CompilationStatus") or {}
        ret: dict[str, Any] = {
            "id": rule.get("Id"),
            "guid": rule.get("ObjectId"),
            "folder_id": (rule.get("Folder") or {}).get("Id"),
            "origin_id": rule.get("OriginId"),
            "name": rule.get("SystemName"),
            "formula": rule.get("Formula"),
            "groups": rule_groups,
            "localization_rules": rule.get("LocalizationRules"),
            "compilation_sdk": compilation_status.get("SdkVersion"),
            "compilation_status": compilation_status.get("CompilationStatusId"),
            "deployment_status": str(rule.get("DeploymentStatus") or "").lower(),
        }
        ret["hash"] = sha256(str(ret.get("formula", "")).encode("utf-8")).hexdigest()

        self.log.info(
            f'status=success, action=get_rule, msg="Got rule {rule_id}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

        return ret

    def get_table_info(self, db_name: str | None, table_id: str) -> dict:
        """Получить описание табличного списка.

        :param db_name: Имя БД (None - первая deployable)
        :param table_id: KB ID табличного списка
        :return: {'param1': value, 'param2': value}
        """
        db_name = self.__resolve_db_name(db_name)

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        api_url = f"{self.__api_table_info}/{table_id}"
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{api_url}"

        r = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            headers=headers,
        )
        table = r.json()

        table_groups = [
            {"id": i.get("Id"), "name": i.get("SystemName")}
            for i in table.get("Groups") or []
        ]

        table_fields = [
            {
                "name": i.get("Name"),
                "mapping": i.get("Mapping"),
                "type_id": i.get("TypeId"),
                "primary_key": i.get("IsPrimaryKey"),
                "indexed": i.get("IsIndex"),
                "nullable": i.get("IsNullable"),
            }
            for i in table.get("Fields") or []
        ]

        fill_type = table.get("FillType")
        deployment_status = table.get("DeploymentStatus")
        ret: dict[str, Any] = {
            "id": table.get("Id"),
            "guid": table.get("ObjectId"),
            "folder_id": (table.get("Folder") or {}).get("Id"),
            "origin_id": table.get("OriginId"),
            "name": table.get("SystemName"),
            "size_max": table.get("MaxSize"),
            "size_typical": table.get("TypicalSize"),
            "ttl": table.get("Ttl"),
            "fields": table_fields,
            "description": table.get("Description"),
            "groups": table_groups,
            "fill_type": fill_type.lower() if isinstance(fill_type, str) else fill_type,
            "pdql": table.get("PdqlQuery"),
            "asset_groups": table.get("AssetGroups"),
            "deployment_status": deployment_status.lower()
            if isinstance(deployment_status, str)
            else deployment_status,
        }

        self.log.info(
            f'status=success, action=get_table_info, msg="Got table {table_id}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

        return ret

    def get_table_data(
        self, db_name: str | None, table_id: str, filters: dict | None = None
    ) -> Iterator[dict]:
        """Получить содержимое табличного из KB. В KB только справочники могут
        содержать записи. Для доступа к данным иных типов таблиц необходимо
        использовать class Table.

        :param db_name: Имя БД (None - первая deployable)
        :param table_id: KB ID табличного списка
        :param filters: KB фильтр записей в таблице. Спецификацию можно
            найти путем реверса WEB API
        :return: Iterator
        """
        db_name = self.__resolve_db_name(db_name)
        api_url = f"{self.__api_table_info}/{table_id}/rows"

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{api_url}"
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        params = {"sort": None}

        if filters is not None:
            params.update(filters)

        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.kb_objects_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__iterate_table_rows(url, params, headers, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            for i in ret:
                line_counter += 1
                i.pop("Id", None)
                yield i
        took_time = get_metrics_took_time(start_time)

        self.log.info(
            'status=success, action=get_table_data, msg="Query executed, response have been read", '
            f'hostname="{self.__kb_hostname}", lines={line_counter}, db="{db_name}"'
        )
        self.log.info(
            f'hostname="{self.__kb_hostname}", metric=get_table_data, took={took_time}ms, lines={line_counter}'
        )

    def __iterate_table_rows(
        self,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        offset: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        params["skip"] = offset
        params["take"] = limit
        rq = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )
        response: dict[str, Any] | None = rq.json()
        if response is None or "Rows" not in response:
            self.log.error(
                'status=failed, action=kb_objects_iterate, msg="KB data request return None or '
                'has wrong response structure", '
                f'hostname="{self.__kb_hostname}"'
            )
            raise RuntimeError(
                "KB data request return None or has wrong response structure"
            )

        return list(response["Rows"])

    def create_folder(
        self, db_name: str | None, name: str, parent_id: str | None = None
    ) -> str:
        """Создать папку для контента.

        :param db_name: Имя БД (None - первая deployable)
        :param name: Имя создаваемой папки
        :param parent_id: Идентификатор родительской папки
        :return: ID созданной папки
        """
        db_name = self.__resolve_db_name(db_name)
        params: dict[str, Any] = {"name": name, "parentId": parent_id}
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_folders}"

        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )

        if r.status_code != 201:
            self.log.error(
                f'status=failed, action=create_folder, msg="failed to create folder {name}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}", response="{r.text}"'
            )
            raise RuntimeError(f"Failed to create folder {name}: {r.text}")

        folder_id = str(r.json())
        self.get_folders_list(db_name, do_refresh=True)
        folder_path = self.get_folder_path_by_id(db_name, folder_id)
        self.log.info(
            f'status=success, action=create_folder, msg="created folder {folder_path} with id {folder_id}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

        return folder_id

    def delete_folder(self, db_name: str | None, folder_id: str) -> requests.Response:
        """Удалить папку.

        :param db_name: Имя БД (None - первая deployable)
        :param folder_id: ID удаляемой папки
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        folder_path = self.get_folder_path_by_id(db_name, folder_id)

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_folders}/{folder_id}"

        r = exec_request(
            self.__kb_session,
            url,
            method="DELETE",
            timeout=self.settings.connection_timeout,
            headers=headers,
        )

        if r.status_code == 204:
            self.log.info(
                f'status=success, action=delete_folder, msg="deleted folder {folder_path} with id {folder_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
            self.get_folders_list(db_name, do_refresh=True)
        else:
            self.log.error(
                f'status=failed, action=delete_folder, msg="failed to delete folder {folder_path} with id {folder_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return r

    def create_co_rule(
        self,
        db_name: str | None,
        name: str,
        code: str,
        ru_desc: str | None,
        folder_id: str,
        group_ids: list[str] | None = None,
    ) -> str:
        """Создать правило корреляции.

        MP SIEM 26.0: поле ``groupsToSave`` в теле создания эндпоинт игнорирует
        (созданный объект остаётся непривязанным), поэтому наборы установки
        привязываются отдельно через ``link_content_to_groups``.

        :param db_name: Имя БД (None - первая deployable)
        :param name: имя создаваемого правила корреляции
        :param code: код правила
        :param ru_desc: описание в русской локали
        :param folder_id: ID каталога, в который разместить правило
        :param group_ids: ID наборов установки, в которые включить
            правило
        :return: ID созданного правила
        """
        db_name = self.__resolve_db_name(db_name)
        params: dict[str, Any] = {
            "systemName": name,
            "formula": code,
            "description": {"RUS": ru_desc} if ru_desc else {},
            "folderId": folder_id,
            "groupsToSave": group_ids or [],
            "localizationRulesToAdd": [],
            "mappingConflictAction": "exception",
        }

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_co_rules}"

        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )

        if r.status_code != 201:
            self.log.error(
                f'status=failed, action=create_co_rule, msg="failed to create co rule {name}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}", response="{r.text}"'
            )
            raise RuntimeError(f"Failed to create correlation rule {name}: {r.text}")

        rule_id = str(r.json())
        if group_ids:
            self.link_content_to_groups(db_name, [rule_id], group_ids)
        self.log.info(
            f'status=success, action=create_co_rule, msg="created co rule {name} with id {rule_id}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

        return rule_id

    def create_group(
        self, db_name: str | None, name: str, parent_id: str | None = None
    ) -> str:
        """Создать набор установки.

        MP SIEM 26.0: наборы установки - классы правил. Контракт
        `CoreApi.RuleClasses.yaml` `POST /api/RuleClasses/` (AddRuleClass).

        :param db_name: Имя БД (None - первая deployable)
        :param name: Имя набора установки
        :param parent_id: ID родительского набора установки. По умолчанию
            набор создается в корне дерева (узел 'Все объекты'), иначе UI KB
            набор не отображает.
        :return: ID созданного набора установки
        """
        db_name = self.__resolve_db_name(db_name)
        params: dict[str, Any] = {
            "SystemName": name,
            "ParentRuleClassId": parent_id or self.GROUPS_ROOT_ID,
            "Locales": [],
        }

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_content_rule_classes}"

        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            params={"contentDatabase": db_name},
            json=params,
        )

        if r.status_code != 201:
            self.log.error(
                f'status=failed, action=create_group, msg="failed to create group {name}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}", response="{r.text}"'
            )
            raise RuntimeError(f"Failed to create group {name}: {r.text}")

        group_id = str(r.json())
        self.get_groups_list(db_name, do_refresh=True)
        group_path = self.get_group_path_by_id(db_name, group_id)
        self.log.info(
            f'status=success, action=create_group, msg="created group {group_path} with id {group_id}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

        return group_id

    def delete_group(self, db_name: str | None, group_id: str) -> requests.Response:
        """Удалить набор установки.

        MP SIEM 26.0: наборы установки - классы правил. Контракт
        `CoreApi.RuleClasses.yaml` `DELETE /api/RuleClasses/{ruleClassId}`
        (RemoveRuleClass). KB отвечает 500 на попытку удалить набор с
        присвоенными объектами, поэтому непустой набор сначала требуется
        очистить (`link_content_to_groups`).

        :param db_name: Имя БД (None - первая deployable)
        :param group_id: ID удаляемого набора установки
        :return: Объект Response
        :raises RuntimeError: если набор содержит объекты
        """
        db_name = self.__resolve_db_name(db_name)
        group_name = self.get_group_path_by_id(db_name, group_id)

        if not self.is_group_empty(db_name, group_id):
            self.log.error(
                f'status=failed, action=delete_group, msg="group {group_name} is not empty, '
                f'detach objects first", hostname="{self.__kb_hostname}", '
                f'db="{db_name}", group_id="{group_id}"'
            )
            raise RuntimeError(
                f"Group {group_name} ({group_id}) is not empty: "
                f"detach objects via link_content_to_groups before deleting"
            )

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_content_rule_classes}/{group_id}"

        r = exec_request(
            self.__kb_session,
            url,
            method="DELETE",
            timeout=self.settings.connection_timeout,
            headers=headers,
            params={"contentDatabase": db_name},
        )

        if r.status_code == 204:
            self.log.info(
                f'status=success, action=delete_group, msg="deleted group {group_name} with id {group_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
            self.get_groups_list(db_name, do_refresh=True)
        else:
            self.log.error(
                f'status=failed, action=delete_group, msg="failed to delete group {group_name} with id {group_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return r

    def __find_group_node(
        self, nodes: list[dict[str, Any]], group_id: str
    ) -> dict[str, Any] | None:
        """Найти узел набора установки в дереве SiemObjectGroup/values."""
        for node in nodes:
            if node.get("Id") == group_id:
                return node
            found = self.__find_group_node(node.get("Children") or [], group_id)
            if found is not None:
                return found
        return None

    def is_group_empty(self, db_name: str | None, group_id: str) -> bool:
        """Проверить есть ли данные в наборе установки.

        MP SIEM 26.0: `objects/list` параметр `groupId` игнорирует, поэтому
        состав набора по нему не узнать. Используется три--state индикатор
        `POST /api-studio/siem/mass-operations/SiemObjectGroup/values`: для
        набора с объектами `AssignedTo` = `Some`/`All`, для пустого - пусто.
        Проверка охватывает и вложенные наборы (как прежний `recursive=True`).

        :param db_name: Имя БД (None - первая deployable)
        :param group_id: идентификатор набора установки
        :return: True - если в наборе установки (и его дочерних наборах) нет
            контента
        """
        db_name = self.__resolve_db_name(db_name)
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        params: dict[str, Any] = {
            "selectedObjects": {"ids": [], "selectionMode": "Selected"},
            "filter": {
                "filters": None,
                "search": "",
                "folderId": None,
                "groupId": None,
                "recursive": True,
            },
        }

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_siem_objgroups_values}"

        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )

        if r.status_code != 201:
            self.log.error(
                f'status=failed, action=is_group_empty, msg="failed to get group {group_id} state", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
            raise RuntimeError(f"Failed to get group {group_id} state: {r.text}")

        def has_assigned(group_node: dict[str, Any]) -> bool:
            if group_node.get("AssignedTo"):
                return True
            return any(has_assigned(c) for c in group_node.get("Children") or [])

        node = self.__find_group_node(r.json(), group_id)
        if node is None:
            raise KeyError(f"Group not found: {group_id}")

        empty = not has_assigned(node)

        self.log.info(
            f'status=success, action=is_group_empty, msg="group {group_id} '
            f'{"is empty" if empty else "has objects"}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

        return empty

    def get_group_path_by_id(self, db_name: str | None, folder_id: str) -> str:
        """Получить путь в дереве наборов установки по идентификатору набора
        установки.

        Корневой узел ('Все объекты') в путь не включается.

        :param db_name: Имя БД (None - первая deployable)
        :param folder_id: идентификатор набора установки
        :return: путь в дереве наборов установки вида
            root/child/grandchild
        """
        db_name = self.__resolve_db_name(db_name)
        groups = self.get_groups_list(db_name)

        if folder_id not in groups:
            raise KeyError(f"Group not found: {folder_id}")

        if folder_id == self.GROUPS_ROOT_ID:
            return ""

        parent = groups[folder_id]["parent_id"]
        name = str(groups[folder_id]["name"])
        ret_path = self.get_group_path_by_id(db_name, parent) if parent else ""
        return f"{ret_path}/{name}" if ret_path else name

    def get_group_id_by_path(self, db_name: str | None, search_path: str) -> str:
        """Получить идентификатор набора установки по пути в дереве.

        :param db_name: Имя БД (None - первая deployable)
        :param search_path: Путь в формате root/child/grandchild
        :return: идентификатор набора установки
        """
        db_name = self.__resolve_db_name(db_name)
        groups = self.get_groups_list(db_name)
        path_index: dict[str, str] = {}
        for current_id in groups:
            path = self.get_group_path_by_id(db_name, current_id)
            path_index[path] = current_id

        return path_index.get(search_path, "")

    def __get_group_children_tree(
        self, groups: dict[str, dict[str, Any]], current_id: str
    ) -> list[str]:
        children = list(groups[current_id].get("children_ids", []))
        retval = list(children)
        for child in children:
            retval.extend(self.__get_group_children_tree(groups, child))

        return retval

    def get_nested_group_ids(self, db_name: str | None, group_id: str) -> list[str]:
        """Получить идентификаторы дочерних наборов установки.

        :param db_name: Имя БД (None - первая deployable)
        :param group_id: идентификатор группы
        :return: список идентификаторов дочерних наборов установки
        """
        db_name = self.__resolve_db_name(db_name)
        groups = dict(self.get_groups_list(db_name))
        for current_id, group_data in groups.items():
            parent_id = group_data["parent_id"]
            if parent_id:
                if "children_ids" not in groups[parent_id]:
                    groups[parent_id]["children_ids"] = []

                groups[parent_id]["children_ids"].append(current_id)

        return self.__get_group_children_tree(groups, group_id)

    def export_group(
        self,
        db_name: str | None,
        group_id: str,
        local_filepath: str,
        export_format: str | None = EXPORT_FORMAT_KB,
    ) -> int:
        """Экспортировать набор установки.

        MP SIEM 26.0: групповой экспорт переехал с ``POST
        /api-studio/siem/export`` (``mode`` стал ``DeployModeEnum`` и групп не
        поддерживает) на контракт CoreApi.SiemExport:

        - KB:   ``GET /api/siem/export/kbpackage?contentMode=deploymentSet&groupId=...``
        - SIEM: ``GET /api/siem/export/{groupId}``

        Оба отдают ``application/octet-stream`` и реально ограничены набором.

        :param db_name: Имя БД (None - первая deployable)
        :param group_id: ID набора установки
        :param local_filepath: файл в который сохранить набор установки
        :param export_format: формат экспорта (KB / SIEM Lite)
        :return: размер созданного файла
        """
        db_name = self.__resolve_db_name(db_name)
        group_path = self.get_group_path_by_id(db_name, group_id)

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        if export_format == self.EXPORT_FORMAT_SIEM_LITE:
            url = (
                f"https://{self.__kb_hostname}:{self.__kb_port}"
                f"{self.__api_contract_export}/{group_id}"
            )
            params: dict[str, Any] = {"contentDatabase": db_name}
        else:
            url = (
                f"https://{self.__kb_hostname}:{self.__kb_port}"
                f"{self.__api_contract_export}/kbpackage"
            )
            params = {
                "contentMode": "deploymentSet",
                "groupId": group_id,
                "contentDatabase": db_name,
            }

        r = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            headers=headers,
            params=params,
        )

        retval = 0
        if r.status_code == 200:
            # Не экспортировать пустой пак (в него попадают все ПТшные макросы)
            is_group_empty = self.is_group_empty(db_name, group_id)
            if not is_group_empty:
                with open(local_filepath, "wb") as kbfile:
                    retval = kbfile.write(r.content)

                self.log.info(
                    f'status=success, action=export_group, msg="group {group_path} with id {group_id} exported to {local_filepath}", '
                    f'hostname="{self.__kb_hostname}", db="{db_name}"'
                )
            else:
                self.log.info(
                    f'status=success, action=export_group, msg="group {group_path} with id {group_id} is empty", '
                    f'hostname="{self.__kb_hostname}", db="{db_name}"'
                )

        else:
            self.log.error(
                f'status=failed, action=export_group, msg="failed to export group {group_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return retval

    def import_group(
        self,
        db_name: str | None,
        filepath: str,
        mode: str | None = IMPORT_ADD_AND_UPDATE,
    ) -> int:
        """Импортировать набор установки.

        :param db_name: Имя БД (None - первая deployable)
        :param filepath: имя файла набора установки
        :param mode: режим импорта
        :return: response_code
        """
        db_name = self.__resolve_db_name(db_name)
        headers = {
            "Content-Database": db_name,
            "Content-Locale": "RUS",
            "Content-Type": "application/octet-stream",
        }

        filename = os.path.basename(filepath)

        url = (
            f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_temp_file_storage_upload}?"
            f"fileName={filename}&storageType=Temp"
        )

        uploaded_id = ""
        with open(filepath, "rb") as kbfile:
            r = exec_request(
                self.__kb_session,
                url,
                method="POST",
                timeout=self.settings.connection_timeout,
                headers=headers,
                data=kbfile,
            )

            if r.status_code == 201:
                # Upload successful
                uploaded_id = str(r.json().get("UploadId"))
                self.log.info(
                    f'status=success, action=upload_file, msg="file {filepath} uploaded", '
                    f'hostname="{self.__kb_hostname}", db="{db_name}"'
                )
            else:
                self.log.error(
                    f'status=failed, action=upload_file, msg="failed to upload file {filepath}", '
                    f'hostname="{self.__kb_hostname}", db="{db_name}"'
                )

        if uploaded_id:
            # make import

            headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

            params = {"importMacros": False, "mode": mode, "uploadId": uploaded_id}

            url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_import}"

            r = exec_request(
                self.__kb_session,
                url,
                method="POST",
                timeout=self.settings.connection_timeout,
                headers=headers,
                json=params,
            )

            if r.status_code == 201:
                # Upload successful
                self.log.info(
                    f'status=success, action=import_file, msg="file {filepath} imported", '
                    f'hostname="{self.__kb_hostname}", db="{db_name}"'
                )
            else:
                self.log.error(
                    f'status=failed, action=import_file, msg="failed to import file {filepath}", '
                    f'hostname="{self.__kb_hostname}", db="{db_name}"'
                )

            return r.status_code
        else:
            return -1

    def create_group_path(self, db_name: str | None, group_path: str) -> str:
        """Последовательное создание пути в дереве наборов установки.

        :param db_name: Имя БД (None - первая deployable)
        :param group_path: путь в дереве наборов установки
        :return: идентификатор листового набора установки
        """
        db_name = self.__resolve_db_name(db_name)
        path_parts = group_path.split("/")
        for i in range(1, len(path_parts) + 1):
            parent_path = "/".join(path_parts[: i - 1])
            path = "/".join(path_parts[0:i])
            group_id = self.get_group_id_by_path(db_name, path)
            if not group_id:
                parent_group_id = (
                    self.get_group_id_by_path(db_name, parent_path) or None
                )
                self.create_group(db_name, path_parts[i - 1], parent_group_id)

        return self.get_group_id_by_path(db_name, group_path)

    def __get_linked_ids(self, objects: list[dict[str, Any]]) -> list[str]:
        """Разбор ответа на запрос привязанных наборов установки.

        :param objects: Ответ API MPSIEM
        :return: список идентификаторов наборов установки
        """
        linked: list[str] = []
        for item in objects:
            if item.get("AssignedTo") == "All" and "Id" in item:
                linked.append(item["Id"])
            if item.get("Children"):
                linked.extend(self.__get_linked_ids(item["Children"]))

        return linked

    def get_linked_groups(self, db_name: str | None, content_item_id: str) -> list[str]:
        """Получить список идентификаторов связанных наборов установки для
        элемента контента.

        :param db_name: Имя БД (None - первая deployable)
        :param content_item_id: идентификатор контента
        :return: список идентификаторов связанных наборов установки
        """
        db_name = self.__resolve_db_name(db_name)
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        params: dict[str, Any] = {
            "selectedObjects": {
                "ids": [
                    content_item_id,
                ],
                "selectionMode": "Selected",
            },
            "filter": None,
        }

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_siem_objgroups_values}"

        r = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )

        if r.status_code == 201:
            group_ids = self.__get_linked_ids(r.json())

            self.log.info(
                f'status=success, action=get_linked_groups, msg="Item {content_item_id} linked to groups {group_ids}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

            return group_ids

        self.log.error(
            f'status=failed, action=get_linked_groups, msg="can not get group links for {content_item_id}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )
        return []

    def link_content_to_groups(
        self,
        db_name: str | None,
        content_items_ids: list[str],
        group_ids: list[str],
        remove_group_ids: list[str] | None = None,
    ) -> None:
        """Связать идентификаторы контента с идентификаторами наборов
        установки.

        MP SIEM 26.0: единственный работающий механизм привязки - ``PUT
        /api-studio/siem/mass-operations`` (операция ``SiemObjectGroup``).
        Контрактный ``POST /api/RuleClasses/{id}/bulk-add`` на 26.0 отвечает
        204, но привязку не выполняет (см. ``bulk_add_rule_class``).

        :param db_name: Имя БД (None - первая deployable)
        :param content_items_ids: идентификаторы контента
        :param group_ids: идентификаторы наборов установки (добавить)
        :param remove_group_ids: идентификаторы наборов установки (отвязать)
        :raises ValueError: если не указано ни одного набора для
            добавления/удаления
        """
        db_name = self.__resolve_db_name(db_name)
        group_ids = group_ids or []
        remove_group_ids = remove_group_ids or []

        if not group_ids and not remove_group_ids:
            raise ValueError(
                "Both group_ids and remove_group_ids are empty, nothing to do"
            )

        # получить имя набора установки с которым осуществляется связывание
        group_names = [
            group_data.get("name", "")
            for group_id, group_data in self.get_groups_list(db_name).items()
            if group_id in group_ids
        ]

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}

        params: dict[str, Any] = {
            "Operations": [
                {
                    "Id": "SiemObjectGroup",
                    "ValuesToSave": group_ids,
                    "ValuesToRemove": remove_group_ids,
                }
            ],
            "Entities": {
                "selectedObjects": {
                    "ids": content_items_ids,
                    "selectionMode": "Selected",
                },
                "filter": {},
            },
        }

        url = (
            f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_mass_operations}"
        )

        r = exec_request(
            self.__kb_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )

        if r.status_code == 200:
            self.log.info(
                f'status=success, action=link_content_to_groups, msg="{content_items_ids!s} linked to {group_names!s}", '
                f"unlinked from {remove_group_ids!s}, "
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
        else:
            self.log.error(
                f'status=failed, action=link_content_to_groups, msg="can not link {content_items_ids!s} to {group_names!s}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

    def process_kb_metadata(
        self,
        db_name: str | None,
        obj_map: dict[tuple[str, str], str],
        kb_meta: dict[str, Any],
    ) -> None:
        db_name = self.__resolve_db_name(db_name)
        if kb_meta.get("group_path"):
            # Создать путь в дереве наборов установки
            group_id = self.create_group_path(db_name, kb_meta["group_path"])

            # Есть связанные элементы контента
            if kb_meta.get("kb_tree"):
                contend_guid_strs = []
                for content_type in kb_meta["kb_tree"]:
                    for content_path in kb_meta["kb_tree"][content_type]:
                        key = (content_type, content_path)

                        # Пробуем найти маппинг (Type, Path)->GUID
                        if key in obj_map:
                            contend_guid_strs.append(obj_map[key])
                        else:
                            self.log.error(
                                f'status=failed, action=map_id_to_guid, msg="can not find object {key}", '
                                f'hostname="{self.__kb_hostname}", db="{db_name}"'
                            )

                if contend_guid_strs:
                    # Связать контент с набором установки
                    self.link_content_to_groups(
                        db_name,
                        contend_guid_strs,
                        [
                            group_id,
                        ],
                    )

    def get_folder_path_by_id(self, db_name: str | None, folder_id: str) -> str:
        """Получить путь в дереве папок по ID папки.

        :param db_name: имя БД (None - первая deployable)
        :param folder_id: ID папки
        :return: путь в дереве папок
        """
        db_name = self.__resolve_db_name(db_name)
        folders = self.get_folders_list(db_name)
        if folder_id not in folders:
            raise KeyError(f"Folder not found: {folder_id}")

        parent = folders[folder_id]["parent_id"]
        name = str(folders[folder_id]["name"])
        ret_path = self.get_folder_path_by_id(db_name, parent) if parent else ""
        return f"{ret_path}/{name}" if ret_path else name

    def get_folder_id_by_path(self, db_name: str | None, path: str) -> str:
        """Получить ID папки по пути в дереве папок.

        :param db_name: имя БД (None - первая deployable)
        :param path: путь в дереве папок
        :return: ID папки
        """
        db_name = self.__resolve_db_name(db_name)
        folders = self.get_folders_list(db_name)
        path_to_id_map: dict[str, str] = {
            self.get_folder_path_by_id(db_name, folder_id): folder_id
            for folder_id in folders
        }
        return path_to_id_map.get(path, "")

    def get_content_data_by_folder_id(
        self, db_name: str | None, folder_id: str
    ) -> dict[str, Any]:
        """Получить данные по контенту лежащему в папке с заданным ID.

        :param db_name: имя БД (None - первая deployable)
        :param folder_id: ID папки
        :return: словарь вида {'ID объекта' : 'Тип объекта'}
        """
        db_name = self.__resolve_db_name(db_name)
        filters: dict[str, Any] = {
            "folderId": folder_id,
            "filters": None,
            "search": "",
            "sort": [{"name": "objectId", "order": 0, "type": 0}],
            "recursive": False,
            "groupId": None,
            "withoutGroups": False,
        }
        content = list(self.get_all_objects(db_name, filters))
        nested_data: dict[str, Any] = {
            item["id"]: item["object_kind"]
            for item in content
            if item["folder_id"] == folder_id
        }
        return nested_data

    def get_nested_folder_ids_by_folder_id(
        self, db_name: str | None, folder_id: str
    ) -> list[str]:
        """Получить идентификаторы дочерних папок по ID папки.

        :param db_name: имя БД (None - первая deployable)
        :param folder_id: ID папки
        :return: идентификаторы вложенных папок
        """
        db_name = self.__resolve_db_name(db_name)
        folders = self.get_folders_list(db_name)
        return [
            fold_id
            for fold_id, fold_data in folders.items()
            if fold_data["parent_id"] == folder_id
        ]

    def move_folder(
        self, db_name: str | None, folder_id: str, dst_folder_id: str
    ) -> requests.Response:
        """Переместить папку под другого родителя.

        :param db_name:Имя БД (None - первая deployable)
        :param folder_id:
        :param dst_folder_id:
        :return:
        """
        db_name = self.__resolve_db_name(db_name)

        folder_path = self.get_folder_path_by_id(db_name, folder_id)
        dst_folder_path = self.get_folder_path_by_id(db_name, dst_folder_id)
        folder_name = folder_path.split("/")[-1]

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_folders}/{folder_id}"

        params: dict[str, Any] = {
            "id": folder_id,
            "name": folder_name,
            "parentId": dst_folder_id,
        }

        r = exec_request(
            self.__kb_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=params,
        )

        if r.status_code == 200:
            self.log.info(
                f'status=success, action=move_folder, msg="folder {folder_path} moved to {dst_folder_path}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
            self.get_folders_list(db_name, do_refresh=True)
        else:
            self.log.error(
                f'status=failed, action=move_folder, msg="failed to move folder {folder_path} to {dst_folder_path}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return r

    def __content_item_url(self, item_id: str, item_type: str) -> str:
        """URL элемента контента в KB по его типу и ID."""
        object_path = self.ITEM_TYPE_MAP.get(item_type)
        if object_path is None:
            raise ValueError(
                f"Unsupported content item type: {item_type}. "
                f"Expected one of {sorted(self.ITEM_TYPE_MAP)}"
            )
        return (
            f"https://{self.__kb_hostname}:{self.__kb_port}"
            f"{self.__api_siem}/{object_path}/{item_id}"
        )

    def get_content_item(
        self, db_name: str | None, item_id: str, item_type: str
    ) -> dict[str, Any]:
        """Получить элемент контента по типу и ID.

        Поле ``DeploymentSets`` ответа содержит наборы установки (RuleClass),
        к которым привязан объект, - быстрый способ проверить привязку без
        выгрузки всего дерева классов (``get_linked_groups`` медленнее на
        больших БД).

        :param db_name: имя БД (None - первая deployable)
        :param item_id: ID элемента контента
        :param item_type: тип (CorrelationRule/AggregationRule/EnrichmentRule/NormalizationRule/TabularList)
        :return: dict с полями для элемента контента
        """
        db_name = self.__resolve_db_name(db_name)

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = self.__content_item_url(item_id, item_type)

        r = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            headers=headers,
        )

        if r.status_code == 200:
            self.log.info(
                f'status=success, action=get_content_item, msg="Get item type {item_type} item id {item_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
        else:
            self.log.error(
                f'status=failed, action=get_content_item, msg="Failed to get item type {item_type} item id {item_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
            raise RuntimeError(f"Failed to get content item {item_id}: {r.text}")

        return cast(dict[str, Any], r.json())

    def move_content_item(
        self, db_name: str | None, item_id: str, item_type: str, dst_folder_id: str
    ) -> requests.Response:
        """Переместить элемент контента в другую папку.

        :param db_name: имя БД (None - первая deployable)
        :param item_id: ID элемента контента
        :param item_type: тип контента
        :param dst_folder_id: (CorrelationRule/AggregationRule/EnrichmentRule/NormalizationRule/TabularList)
        :return:
        """
        db_name = self.__resolve_db_name(db_name)

        dst_folder_path = self.get_folder_path_by_id(db_name, dst_folder_id)

        item_data = self.get_content_item(db_name, item_id, item_type)
        item_name = item_data.get("SystemName", "Unknown")

        put_content: dict[str, Any] = {
            "folderId": dst_folder_id,
            "description": {"RUS": item_data.get("Description")}
            if item_data.get("Description")
            else {},
        }

        if item_type == "TabularList":
            put_content.update(
                {
                    "userCanEditContent": item_data.get("UserCanEditContent"),
                    "fields": [
                        {
                            "id": d["Id"],
                            "name": d["Name"],
                            "typeId": d["TypeId"],
                            "isPrimaryKey": d["IsPrimaryKey"],
                            "isIndex": d["IsIndex"],
                            "isNullable": d["IsNullable"],
                            "mapping": d["Mapping"],
                        }
                        for d in item_data.get("Fields") or []
                    ],
                }
            )
        else:
            put_content.update(
                {
                    "systemName": item_data.get("SystemName"),
                    "formula": item_data.get("Formula"),
                }
            )

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = self.__content_item_url(item_id, item_type)

        r = exec_request(
            self.__kb_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            headers=headers,
            json=put_content,
        )

        if r.status_code == 200:
            self.log.info(
                f'status=success, action=move_content_item, msg={item_type} {item_name} moved to {dst_folder_path}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
        else:
            self.log.error(
                f'status=failed, action=move_content_item, msg="Failed to move {item_type} {item_name} to {dst_folder_path}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return r

    def delete_content_item(
        self, db_name: str | None, item_id: str, item_type: str
    ) -> requests.Response:
        """Удаление элемента контента.

        :param db_name:  Имя БД (None - первая deployable)
        :param item_id: ID элемента контента
        :param item_type: тип (CorrelationRule/AggregationRule/EnrichmentRule/NormalizationRule/TabularList)
        :return:
        """
        db_name = self.__resolve_db_name(db_name)

        item_data = self.get_content_item(db_name, item_id, item_type)
        item_name = item_data.get("SystemName", "Unknown")

        # В 26.0 удаление объекта из KB не требует явной per-object
        # деинсталляции из SIEM: uninstall-эндпоинта нет, а `deploy` -
        # push-модель (весь pending-контент на конвейер). Оставшиеся в SIEM
        # следы убираются следующим `deploy_sync` после удаления из KB.

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = self.__content_item_url(item_id, item_type)

        r = exec_request(
            self.__kb_session,
            url,
            method="DELETE",
            timeout=self.settings.connection_timeout,
            headers=headers,
        )

        if r.status_code == 204:
            self.log.info(
                f'status=success, action=delete_content_item, msg="deleted {item_type} {item_name} with id {item_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
        else:
            self.log.error(
                f'status=failed, action=delete_content_item, msg="failed to delete {item_type} {item_name} with id {item_id}", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return r

    def move_folder_content(
        self, db_name: str | None, src_folder_path: str, dst_folder_path: str
    ) -> None:
        """Переместить всю начинку папки (дочерние папки и контент) в другую
        папку.

        :param db_name: имя БД (None - первая deployable)
        :param src_folder_path: путь до исходной папки
        :param dst_folder_path: путь до папки назначения
        :return:
        """
        db_name = self.__resolve_db_name(db_name)
        src_folder_id = self.get_folder_id_by_path(db_name, src_folder_path)
        dst_folder_id = self.get_folder_id_by_path(db_name, dst_folder_path)
        if not src_folder_id:
            raise ValueError(f"Source folder not found: {src_folder_path}")
        if not dst_folder_id:
            raise ValueError(f"Destination folder not found: {dst_folder_path}")

        child_folders = self.get_nested_folder_ids_by_folder_id(db_name, src_folder_id)
        child_content_items = self.get_content_data_by_folder_id(db_name, src_folder_id)

        # Move folders
        for child_folder_id in child_folders:
            self.move_folder(db_name, child_folder_id, dst_folder_id)

        # Move content items
        for child_content_id, child_content_type in child_content_items.items():
            self.move_content_item(
                db_name, child_content_id, child_content_type, dst_folder_id
            )

    def get_content_items_by_group_id(
        self, db_name: str, group_id: str, recursive: bool = True
    ) -> list[dict[str, Any]]:
        """Получить элементы контента по ID набора установки.

        MP SIEM 26.0: API KB больше не отдаёт состав набора установки:
        `objects/list` игнорирует `groupId`, а
        `mass-operations/SiemObjectGroup/values` - только три--state
        индикатор (пусто/частично/полностью) и не перечисляет объекты.
        Сервер использует состав internally только при `install_objects_by_group_id`
        и `export_group` (`mode=group`) - они продолжают работать.

        :param db_name: Имя БД
        :param group_id: ID набора установки
        :param recursive: получить элементы контента из дочерних наборов
            установки
        :raises NotImplementedError: состав набора в 26.0 недоступен
        """
        raise NotImplementedError(
            "KB 26.0 does not expose the object list of an installation group. "
            "Use install_objects_by_group_id/export_group (server-side group "
            "resolution) or is_group_empty for the empty/non-empty check."
        )

    def __content_api_request(
        self,
        db_name: str,
        api_path: str,
        action: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        body: Any = None,
        timeout: float | None = None,
    ) -> Any:
        """Запрос к контрактному Content API (CoreApi.*.yaml) на KB-стороне.

        Контракт требует обязательный query-параметр `contentDatabase`, поэтому
        он передаётся и как query, и как заголовок `Content-Database`.

        :param db_name: Имя БД
        :param api_path: Путь эндпоинта
        :param action: Имя действия для логирования
        :param method: HTTP-метод запроса
        :param params: Дополнительные query-параметры
        :param body: Тело запроса (для POST/PUT)
        :param timeout: Тайм-аут запроса (по умолчанию -
            ``settings.connection_timeout``)
        :return: Разобранный JSON-ответ либо текст ответа для пустого тела
        """
        query: dict[str, Any] = {"contentDatabase": db_name}
        if params:
            query.update(params)

        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{api_path}"

        kwargs: dict[str, Any] = {"params": query}
        if body is not None:
            kwargs["json"] = body

        r = exec_request(
            self.__kb_session,
            url,
            method=method,
            timeout=self.settings.connection_timeout if timeout is None else timeout,
            headers=headers,
            **kwargs,
        )

        if not r.content:
            self.log.info(
                f"status=success, action={action}, method={method}, "
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
            return r.text

        payload: Any = r.json()

        if isinstance(payload, list):
            self.log.info(
                f"status=success, action={action}, method={method}, "
                f'msg="Got {len(payload)} items", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
        else:
            self.log.info(
                f"status=success, action={action}, method={method}, "
                f'msg="Got content info", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return payload

    def get_origins_list(self, db_name: str | None = None) -> list[dict[str, Any]]:
        """Получить список источников контента.

        Контракт: `CoreApi.ContentOrigin.yaml` `GET /api/Origins/`
        (GetContentOrigins).

        :param db_name: Имя БД (None - первая deployable)
        :return: [{"id": ..., "system_name": ..., "nickname": ...,
            "name_rus": ..., "name_eng": ..., "is_local": ...,
            "revision": ...}]
        """
        db_name = self.__resolve_db_name(db_name)
        origins = self.__content_api_request(
            db_name, self.__api_content_origins, "get_origins_list"
        )

        return [self.__normalize_origin(o) for o in origins]

    def get_origin(
        self,
        db_name: str | None = None,
        origin_id: str | None = None,
        system_name: str | None = None,
    ) -> dict[str, Any]:
        """Получить источник контента по идентификатору или имени.

        Контракт: `CoreApi.ContentOrigin.yaml` `GET /api/Origins/{originId}`
        (GetContentOrigin) или `GET /api/Origins/Search/{systemName}`
        (FindContentOrigin).

        :param db_name: Имя БД (None - первая deployable)
        :param origin_id: Идентификатор источника контента
        :param system_name: Системное имя источника контента
        :return: Описание источника контента
        """
        db_name = self.__resolve_db_name(db_name)
        if origin_id is None and system_name is None:
            raise ValueError("Either origin_id or system_name must be given")

        if origin_id is not None:
            api_path = f"{self.__api_content_origins}/{origin_id}"
        else:
            api_path = f"{self.__api_content_origins}/Search/{system_name}"

        origin = self.__content_api_request(db_name, api_path, "get_origin")

        return self.__normalize_origin(origin)

    @staticmethod
    def __normalize_origin(origin: dict[str, Any]) -> dict[str, Any]:
        """Привести ответ ContentOrigin к стилю остальных методов модуля."""
        return {
            "id": origin.get("Id"),
            "system_name": origin.get("SystemName"),
            "nickname": origin.get("Nickname"),
            "name_rus": origin.get("LocaleRus"),
            "name_eng": origin.get("LocaleEng"),
            "is_local": origin.get("IsLocal"),
            "revision": origin.get("Revision"),
        }

    def get_rule_classes(self, db_name: str | None = None) -> list[dict[str, Any]]:
        """Получить классы правил.

        Контракт: `CoreApi.RuleClasses.yaml` `GET /api/RuleClasses/`
        (GetRuleClasses).

        :param db_name: Имя БД (None - первая deployable)
        :return: [{"id": ..., "system_name": ..., "parent_id": ...,
            "name": ..., "locales": [...]}]
        """
        db_name = self.__resolve_db_name(db_name)
        classes = self.__content_api_request(
            db_name, self.__api_content_rule_classes, "get_rule_classes"
        )

        return [
            {
                "id": i.get("Id"),
                "system_name": i.get("SystemName"),
                "parent_id": i.get("ParentRuleClassId"),
                "name": KnowledgeBase.__localization_name(i.get("Locales")),
                "locales": i.get("Locales"),
            }
            for i in classes
        ]

    def get_rule_class(self, db_name: str | None, rule_class_id: str) -> dict[str, Any]:
        """Получить класс правила по идентификатору.

        Контракт: `CoreApi.RuleClasses.yaml` `GET /api/RuleClasses/{ruleClassId}`
        (GetRuleClass).

        :param db_name: Имя БД (None - первая deployable)
        :param rule_class_id: Идентификатор класса правил
        :return: Описание класса правил
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_rule_classes}/{rule_class_id}"
        rule_class = self.__content_api_request(db_name, api_path, "get_rule_class")

        return {
            "id": rule_class.get("Id"),
            "system_name": rule_class.get("SystemName"),
            "parent_id": rule_class.get("ParentRuleClassId"),
            "name": KnowledgeBase.__localization_name(rule_class.get("Locales")),
            "locales": rule_class.get("Locales"),
        }

    def get_rule_localizations(
        self, db_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Получить локализации правил.

        Контракт: `CoreApi.RuleLocalizations.yaml` `GET /api/RuleLocalizations/`
        (GetRuleLocalizations).

        Внимание: ответ содержит все локализации всех правил БД (единицы
        мегабайт), поэтому метод имеет смысл вызывать один раз и переиспользовать
        результат.

        :param db_name: Имя БД (None - первая deployable)
        :return: [{"id": ..., "criteria": ..., "name": ..., "locales": [...]}]
        """
        db_name = self.__resolve_db_name(db_name)
        localizations = self.__content_api_request(
            db_name, self.__api_content_rule_localizations, "get_rule_localizations"
        )

        return [
            {
                "id": i.get("Id"),
                "criteria": i.get("Criteria"),
                "name": KnowledgeBase.__localization_name(i.get("Locales")),
                "locales": i.get("Locales"),
            }
            for i in localizations
        ]

    @staticmethod
    def __localization_name(locales: Any) -> str:
        """Имя локали из ответа Content API, предпочитаем RUS."""
        if not isinstance(locales, list):
            return ""

        preferred = ""
        for loc in locales:
            if not isinstance(loc, dict):
                continue
            name = str(loc.get("Name") or "")
            if str(loc.get("Locale") or "").upper() == "RUS":
                return name
            if not preferred:
                preferred = name

        return preferred

    def get_siem_settings_info(self, db_name: str | None = None) -> dict[str, Any]:
        """Получить сведения о настройках SIEM-содержимого (версия SDK).

        Контракт: `CoreApi.SiemSettings.yaml` `GET /api/SiemSettings/info/`
        (GetSiemSettings).

        :param db_name: Имя БД (None - первая deployable)
        :return: {"id": ..., "sdk_version": ...}
        """
        db_name = self.__resolve_db_name(db_name)
        info = self.__content_api_request(
            db_name, self.__api_content_siem_settings_info, "get_siem_settings_info"
        )

        return {
            "id": info.get("Id"),
            "sdk_version": info.get("SdkVersion"),
        }

    def __content_rules_api(self, content_type: str) -> str:
        """Базовый путь контрактного Content API для типа контента.

        :param content_type: Тип объекта MPContentType
        :return: Базовый путь вида ``/api/CorrelationRules``
        :raises NotImplementedError: для типов без контрактного CRUD
        """
        api_path = self.CONTENT_RULE_API_MAP.get(content_type)
        if api_path is None:
            raise NotImplementedError(
                f"Content rules contract API is not supported for {content_type}"
            )
        return api_path

    def list_content_rules(
        self,
        db_name: str | None,
        content_type: str,
        system_name_equals: str | None = None,
        system_name_contains: str | None = None,
        folder_id: str | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Список правил через контрактный Content API (краткая сводка).

        Контракт: ``CoreApi.{Correlation,Aggregation,Enrichment,Normalization}
        Rules.yaml`` ``GET /api/{Type}Rules/`` (Get*Rules). Возвращает DTO без
        Formula — полный код правила отдаёт ``get_rule`` (studio API) либо
        ``get_content_rule``.

        Фильтры применяются на сервере; без фильтра на большой БД (тысячи
        правил) ответ формируется медленно - при необходимости увеличить
        ``timeout``.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentType
        :param system_name_equals: Фильтр точное совпадение системного имени
        :param system_name_contains: Фильтр подстрока системного имени
        :param folder_id: Фильтр по идентификатору каталога
        :param timeout: Тайм-аут запроса (по умолчанию -
            ``settings.connection_timeout``)
        :return: [{"id": ..., "guid": ..., "system_name": ..., "folder_id":
            ..., "origin_id": ..., "deployment_status": ..., "classes": [...],
            "localizations": [...]}]
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = self.__content_rules_api(content_type)
        params: dict[str, Any] = {}
        if system_name_equals is not None:
            params["systemNameEquals"] = system_name_equals
        if system_name_contains is not None:
            params["systemNameContains"] = system_name_contains
        if folder_id is not None:
            params["folderId"] = folder_id

        action = "list_" + content_type.lower() + "_rules"
        rules = self.__content_api_request(
            db_name, api_path, action, params=params or None, timeout=timeout
        )

        return [self.__normalize_content_rule(r) for r in rules]

    def get_content_rule(
        self, db_name: str | None, content_type: str, rule_id: str
    ) -> dict[str, Any]:
        """Полное правило через контрактный Content API.

        Контракт: ``CoreApi.{Correlation,Aggregation,Enrichment,Normalization}
        Rules.yaml`` ``GET /api/{Type}Rules/{id}`` (Get*Rule). В отличие от
        ``get_rule`` (``/api-studio``) возвращает ``Classes``/``Localizations``
        как DTO ``RuleClassCoreDto``/``RuleLocalizationCoreDto``, но не
        возвращает состояние компиляции.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentType
        :param rule_id: KB ID правила
        :return: Справочник с полями правила, включающий ``formula``,
            ``classes``, ``localizations``
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__content_rules_api(content_type)}/{rule_id}"
        action = "get_" + content_type.lower() + "_rule"
        rule = self.__content_api_request(db_name, api_path, action)

        return self.__normalize_content_rule(rule, include_formula=True)

    @staticmethod
    def __normalize_content_rule(
        rule: dict[str, Any], include_formula: bool = False
    ) -> dict[str, Any]:
        """Привести ответ Content API правил к стилю модуля (snake_case)."""
        normalized: dict[str, Any] = {
            "id": rule.get("Id"),
            "guid": rule.get("ObjectId"),
            "system_name": rule.get("SystemName"),
            "folder_id": (rule.get("Folder") or {}).get("Id"),
            "origin_id": rule.get("OriginId"),
            "origin_name": (rule.get("Origin") or {}).get("Name"),
            "deployment_status": rule.get("DeploymentStatus"),
            "copy_of": (rule.get("CopyOf") or {}).get("Name"),
            "classes": [
                {
                    "id": c.get("Id"),
                    "system_name": c.get("SystemName"),
                    "parent_id": c.get("ParentRuleClassId"),
                    "name": KnowledgeBase.__localization_name(c.get("Locales")),
                }
                for c in rule.get("Classes") or []
            ],
            "localizations": [
                {
                    "id": loc.get("Id"),
                    "criteria": loc.get("Criteria"),
                    "name": KnowledgeBase.__localization_name(loc.get("Locales")),
                }
                for loc in rule.get("Localizations") or []
            ],
            "locales": rule.get("Locales") or [],
        }
        if include_formula:
            normalized["formula"] = rule.get("Formula")

        return normalized

    def create_content_rule(
        self, db_name: str | None, content_type: str, body: dict[str, Any]
    ) -> str:
        """Создать правило через контрактный Content API.

        Контракт: ``POST /api/{Type}Rules/`` (Add*Rule). Тело запроса —
        ``*RuleCreateParamsCoreDto`` с PascalCase-ключами (``SystemName``,
        ``Formula``, ``FolderId``, ``Locales``, ``RuleClassIds``; для
        нормализаций — ``IsSuffixRule``; для корреляций — ``JsonFormula``,
        ``JsonFormulaVersion``).

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentType
        :param body: Тело ``*RuleCreateParamsCoreDto``
        :return: ID созданного правила
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = self.__content_rules_api(content_type)
        action = "create_" + content_type.lower() + "_rule"

        rule_id = self.__content_api_request(
            db_name,
            api_path,
            action,
            method="POST",
            body=body,
        )

        return str(rule_id)

    def update_content_rule(
        self,
        db_name: str | None,
        content_type: str,
        rule_id: str,
        body: dict[str, Any],
    ) -> str:
        """Обновить правило через контрактный Content API.

        Контракт: ``PUT /api/{Type}Rules/{id}`` (Change*Rule). Тело —
        ``*RuleEditParamsCoreDto``: ``FolderId``/``SystemName``/``Formula``.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentType
        :param rule_id: KB ID правила
        :param body: Тело ``*RuleEditParamsCoreDto``
        :return: Текст ответа (пустой при 204 No Content)
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__content_rules_api(content_type)}/{rule_id}"
        action = "update_" + content_type.lower() + "_rule"

        return cast(
            str,
            self.__content_api_request(
                db_name,
                api_path,
                action,
                method="PUT",
                body=body,
            ),
        )

    def delete_content_rule(
        self, db_name: str | None, content_type: str, rule_id: str
    ) -> str:
        """Удалить правило через контрактный Content API.

        Контракт: ``DELETE /api/{Type}Rules/{id}`` (Remove*Rule).

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentType
        :param rule_id: KB ID правила
        :return: Текст ответа (пустой при 204 No Content)
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__content_rules_api(content_type)}/{rule_id}"
        action = "delete_" + content_type.lower() + "_rule"

        return cast(
            str,
            self.__content_api_request(
                db_name,
                api_path,
                action,
                method="DELETE",
            ),
        )

    def update_rule_class(
        self,
        db_name: str | None,
        rule_class_id: str,
        body: dict[str, Any],
    ) -> str:
        """Изменить класс правил (набор установки).

        Контракт: ``CoreApi.RuleClasses.yaml`` ``PUT
        /api/RuleClasses/{ruleClassId}`` (ChangeRuleClass). Тело —
        ``RuleClassParamsCoreDto`` (``SystemName``, ``ParentRuleClassId``).

        :param db_name: Имя БД (None - первая deployable)
        :param rule_class_id: ID класса правил
        :param body: Тело ``RuleClassParamsCoreDto``
        :return: Текст ответа (пустой при 204 No Content)
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_rule_classes}/{rule_class_id}"

        return cast(
            str,
            self.__content_api_request(
                db_name,
                api_path,
                "update_rule_class",
                method="PUT",
                body=body,
            ),
        )

    def bulk_add_rule_class(
        self,
        db_name: str | None,
        rule_class_id: str,
        siem_object_ids: list[str],
    ) -> str:
        """Массово присвоить объектам класс правил (на 26.0 - NO-OP).

        Контракт: ``CoreApi.RuleClasses.yaml`` ``POST
        /api/RuleClasses/{ruleClassId}/bulk-add`` (BulkAddRuleClass). Тело —
        ``RuleClassBulkAddCoreDto`` (``SiemObjectIds``).

        MP SIEM 26.0: эндпоинт отвечает ``204 No Content``, но привязку не
        выполняет - объект остаётся в прежних классах (проверено на стенде как
        на KB id, так и на ``ObjectId``). Единственный рабочий механизм
        привязки - ``link_content_to_groups`` (``mass-operations``). Метод
        сохраняет запрос для совместимости сигнатуры, но полагаться на него
        нельзя.

        :param db_name: Имя БД (None - первая deployable)
        :param rule_class_id: ID класса правил
        :param siem_object_ids: идентификаторы объектов (``ObjectId``)
        :return: Текст ответа (пустой при 204 No Content)
        """
        db_name = self.__resolve_db_name(db_name)
        self.log.warning(
            f'status=warning, action=bulk_add_rule_class, msg="bulk-add is a no-op '
            f"on MP SIEM 26.0 (returns 204 without linking); use "
            f'link_content_to_groups instead", hostname="{self.__kb_hostname}", '
            f'db="{db_name}", rule_class_id="{rule_class_id}"'
        )
        api_path = f"{self.__api_content_rule_classes}/{rule_class_id}/bulk-add"

        return cast(
            str,
            self.__content_api_request(
                db_name,
                api_path,
                "bulk_add_rule_class",
                method="POST",
                body={"SiemObjectIds": siem_object_ids},
            ),
        )

    def get_rule_localization(
        self, db_name: str | None, rule_localization_id: str
    ) -> dict[str, Any]:
        """Получить локализацию правила по идентификатору.

        Контракт: ``CoreApi.RuleLocalizations.yaml`` ``GET
        /api/RuleLocalizations/{ruleLocalizationId}`` (GetRuleLocalization).

        :param db_name: Имя БД (None - первая deployable)
        :param rule_localization_id: ID локализации правила
        :return: {"id": ..., "criteria": ..., "name": ..., "locales": [...]}
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_rule_localizations}/{rule_localization_id}"
        loc = self.__content_api_request(db_name, api_path, "get_rule_localization")

        return {
            "id": loc.get("Id"),
            "criteria": loc.get("Criteria"),
            "name": KnowledgeBase.__localization_name(loc.get("Locales")),
            "locales": loc.get("Locales"),
        }

    def __studio_api_request(
        self,
        db_name: str,
        api_path: str,
        action: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        body: Any = None,
        timeout: float | None = None,
    ) -> Any:
        """Запрос к kb-ui Studio API (``/api-studio/...``) на KB-стороне.

        В отличие от ``__content_api_request`` контракт Studio API берёт БД
        только из заголовка ``Content-Database`` (query-параметр не нужен).

        :param db_name: Имя БД
        :param api_path: Путь эндпоинта
        :param action: Имя действия для логирования
        :param method: HTTP-метод запроса
        :param params: Query-параметры
        :param body: Тело запроса (для POST/PUT)
        :param timeout: Тайм-аут запроса (по умолчанию -
            ``settings.connection_timeout``)
        :return: Разобранный JSON-ответ либо текст ответа для пустого тела
        """
        headers = {"Content-Database": db_name, "Content-Locale": "RUS"}
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{api_path}"

        kwargs: dict[str, Any] = {}
        if params:
            kwargs["params"] = params
        if body is not None:
            kwargs["json"] = body

        r = exec_request(
            self.__kb_session,
            url,
            method=method,
            timeout=self.settings.connection_timeout if timeout is None else timeout,
            headers=headers,
            **kwargs,
        )

        if not r.content:
            self.log.info(
                f"status=success, action={action}, method={method}, "
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
            return r.text

        payload: Any = r.json()

        if isinstance(payload, list):
            self.log.info(
                f"status=success, action={action}, method={method}, "
                f'msg="Got {len(payload)} items", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )
        else:
            self.log.info(
                f"status=success, action={action}, method={method}, "
                f'msg="Got content info", '
                f'hostname="{self.__kb_hostname}", db="{db_name}"'
            )

        return payload

    @staticmethod
    def __mass_selection_body(
        object_ids: list[str],
        src_folder_id: str | None = None,
        recursive: bool = False,
        selection_mode: str = "Selected",
        mode: str = "selection",
    ) -> dict[str, Any]:
        """Тело mass-operations (``{mode, selectedObjects, filter}``).

        Сервер 26.0 берёт объекты как пересечение ``selectedObjects.ids`` с
        ``filter``: без покрывающего ``filter.folderId`` (папки, где реально
        лежат объекты) пересечение пусто и операция становится silent no-op
        (``200`` без эффекта у move/delete, ``Success=0`` у check-*).
        ``selectedObjects.ids`` - только GUID (короткий ``ObjectId`` вида
        ``LOC-CR-1`` сервер отвергает с 400). Пустое ``filter`` (без полей)
        сервер тоже принимает, но оно так же не матчит ничего.
        """
        return {
            "mode": mode,
            "selectedObjects": {
                "ids": object_ids,
                "selectionMode": selection_mode,
            },
            "filter": {
                "filters": None,
                "search": "",
                "folderId": src_folder_id,
                "groupId": None,
                "recursive": recursive,
            },
        }

    @staticmethod
    def __content_rule_path(content_type: str) -> str:
        """Суффикс kb-ui пути правил по типу контента (``correlation-rules``)."""
        rule_path = KnowledgeBase.CONTENT_RULE_PATH_MAP.get(content_type)
        if rule_path is None:
            raise ValueError(
                f"Unsupported content type: {content_type}. "
                f"Expected one of {sorted(KnowledgeBase.CONTENT_RULE_PATH_MAP)}"
            )
        return rule_path

    # ------------------------------------------------------------------
    # Группа 1. История просмотра пакетов и справочники фильтров объектов
    # ------------------------------------------------------------------
    def get_knowledge_packs_view_history(
        self, db_name: str | None = None
    ) -> list[dict[str, Any]]:
        """История просмотра knowledge-pack'ов пользователем.

        Контракт kb-ui ``GET /api-studio/siem/knowledge-packs/seen``.

        :param db_name: Имя БД (None - первая deployable)
        :return: [{"object_id": ..., "last_seen_version_hash": ...}]
        """
        db_name = self.__resolve_db_name(db_name)
        seen = self.__studio_api_request(
            db_name,
            self.__api_siem_knowledge_packs_seen,
            "get_knowledge_packs_view_history",
        )

        return [
            {
                "object_id": i.get("ObjectId"),
                "last_seen_version_hash": i.get("LastSeenVersionHash"),
            }
            for i in seen
        ]

    def mark_knowledge_pack_seen(
        self, db_name: str | None, object_id: str, version: str
    ) -> str:
        """Отметить knowledge-pack просмотренным до указанной версии.

        Контракт kb-ui ``PUT /api-studio/siem/knowledge-packs/seen`` телом
        ``{"objectId": ..., "version": ...}``. Идемпотентен (204 без тела).

        :param db_name: Имя БД (None - первая deployable)
        :param object_id: ``ObjectId`` пакета (например ``PT-PKG-34``)
        :param version: Хэш версии (``LastSeenVersionHash``)
        :return: Текст ответа (пустой при 204)
        """
        db_name = self.__resolve_db_name(db_name)
        return cast(
            str,
            self.__studio_api_request(
                db_name,
                self.__api_siem_knowledge_packs_seen,
                "mark_knowledge_pack_seen",
                method="PUT",
                body={"objectId": object_id, "version": version},
            ),
        )

    def get_object_filters(self, db_name: str | None = None) -> list[dict[str, Any]]:
        """Справочник доступных фильтров списка объектов.

        Контракт kb-ui ``GET /api-studio/siem/objects/filters``.

        :param db_name: Имя БД (None - первая deployable)
        :return: [{"id", "name", "type", "multi_select", "placeholder"}]
        """
        db_name = self.__resolve_db_name(db_name)
        filters = self.__studio_api_request(
            db_name, self.__api_siem_objects_filters, "get_object_filters"
        )

        return [
            {
                "id": f.get("Id"),
                "name": f.get("Name"),
                "type": f.get("Type"),
                "multi_select": f.get("MultiSelect"),
                "placeholder": f.get("Placeholder"),
            }
            for f in filters
        ]

    def get_object_filter_values(
        self,
        db_name: str | None,
        filter_id: str,
        search: str = "",
        skip: int = 0,
        take: int = 50,
    ) -> list[dict[str, Any]]:
        """Значения фильтра списка объектов.

        Контракт kb-ui ``POST /api-studio/siem/objects/filters/{filterId}/values``
        (GET отдаёт 405) телом ``{search, skip, take}``.

        :param db_name: Имя БД (None - первая deployable)
        :param filter_id: Идентификатор фильтра (``SiemObjectType``,
            ``ContentType``, ``DeploymentStatus``, ``CompilationStatus``)
        :param search: Подстрока поиска по значению
        :param skip: Пропустить записей
        :param take: Вернуть записей
        :return: [{"id", "name", "assigned_to", "children": [...]}]
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_siem_objects_filters}/{filter_id}/values"
        values = self.__studio_api_request(
            db_name,
            api_path,
            "get_object_filter_values",
            method="POST",
            body={"search": search, "skip": skip, "take": take},
        )

        return [
            {
                "id": v.get("Id"),
                "name": v.get("Name"),
                "assigned_to": v.get("AssignedTo"),
                "children": v.get("Children") or [],
            }
            for v in values
        ]

    # ------------------------------------------------------------------
    # Группа 2. Наборы установки и установка в SIEM
    # ------------------------------------------------------------------
    def get_deployment_sets(
        self, db_name: str | None = None, without_root: bool = False
    ) -> list[dict[str, Any]]:
        """Наборы установки с полной DTO-статусой компиляции/установки.

        Контракт kb-ui ``GET /api-studio/siem/deploymentSets``. В отличие от
        ``get_groups_list`` (плоский список RuleClasses) возвращает узлы с
        ``CompilationStatus``/``DeploymentStatuses``/``GeneralStatus``.

        :param db_name: Имя БД (None - первая deployable)
        :param without_root: Исключить корневой узел (``withoutRoot``)
        :return: [{"id", "parent_set_id", "name", "system_name",
            "general_status", "compilation_status", "deployment_statuses",
            "actions"}]
        """
        db_name = self.__resolve_db_name(db_name)
        params = {"withoutRoot": "true"} if without_root else None
        sets = self.__studio_api_request(
            db_name, self.__api_deployment_sets, "get_deployment_sets", params=params
        )

        return [
            {
                "id": s.get("Id"),
                "parent_set_id": s.get("ParentSetId"),
                "name": s.get("Name"),
                "system_name": s.get("SystemName"),
                "general_status": s.get("GeneralStatus"),
                "compilation_status": s.get("CompilationStatus"),
                "deployment_statuses": s.get("DeploymentStatuses"),
                "actions": s.get("Actions"),
            }
            for s in sets
        ]

    def is_deployment_set_content_exists(
        self, db_name: str | None, group_id: str
    ) -> bool:
        """Есть ли контент в наборе установки.

        Контракт kb-ui ``GET /api-studio/siem/deploymentSets/{groupId}/
        content-exists``.

        :param db_name: Имя БД (None - первая deployable)
        :param group_id: Идентификатор набора установки
        :return: True если в наборе есть контент
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_deployment_sets}/{group_id}/content-exists"
        result = self.__studio_api_request(
            db_name, api_path, "is_deployment_set_content_exists"
        )

        return bool(result.get("Exist"))

    def get_deployment_statistics(self, db_name: str | None = None) -> dict[str, Any]:
        """Сводка установки контента в SIEM.

        Контракт kb-ui ``GET /api-studio/siem/deploy/stats``.

        :param db_name: Имя БД (None - первая deployable)
        :return: {"installed", "not_installed", "outdated",
            "has_successful_deployments"}
        """
        db_name = self.__resolve_db_name(db_name)
        stats = self.__studio_api_request(
            db_name, self.__api_deploy_stats, "get_deployment_statistics"
        )

        return {
            "installed": stats.get("Installed"),
            "not_installed": stats.get("NotInstalled"),
            "outdated": stats.get("Outdated"),
            "has_successful_deployments": stats.get("HasSuccessfulDeployments"),
        }

    def get_pipeline_deployment_status(
        self, db_name: str | None, pipeline_id: str
    ) -> dict[str, Any]:
        """Live-статус конвейера установки (онлайн/прогресс/режим).

        Контракт kb-ui ``GET /api-studio/siem/deploy/{pipelineId}/status``. В
        отличие от ``get_deploy_status`` (разбор журнала деплоя) отдаёт
        текущее состояние конвейера, а не конкретного деплой-процесса.

        :param db_name: Имя БД (None - первая deployable)
        :param pipeline_id: Идентификатор конвейера (``get_pipelines_list``)
        :return: {"percentage", "is_siem_online", "mode", "is_siem_out_of_sync",
            "can_reinstall"}
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_deploy_object}/{pipeline_id}/status"
        status = self.__studio_api_request(
            db_name, api_path, "get_pipeline_deployment_status"
        )

        return {
            "percentage": status.get("Percentage"),
            "is_siem_online": status.get("IsSiemOnline"),
            "mode": status.get("Mode"),
            "is_siem_out_of_sync": status.get("IsSiemOutOfSync"),
            "can_reinstall": status.get("CanReinstall"),
        }

    def get_rules_compilation_statuses(
        self, db_name: str | None, object_ids: list[str]
    ) -> dict[str, int]:
        """Статусы компиляции объектов по списку ID (быстрый поллинг).

        Контракт kb-ui ``POST /api-studio/siem/rules-compilation-statuses``
        (тело - массив ID). Быстрее, чем разворачивать ``get_all_objects`` для
        проверки ``CompilationStatus``. Код статуса - тот же, что в
        ``COMPILATION_STATUS_*``.

        :param db_name: Имя БД (None - первая deployable)
        :param object_ids: Идентификаторы объектов
        :return: {object_id: compilation_status_id}
        """
        db_name = self.__resolve_db_name(db_name)
        statuses = self.__studio_api_request(
            db_name,
            self.__api_siem_rules_compilation_statuses,
            "get_rules_compilation_statuses",
            method="POST",
            body=object_ids,
        )
        casted: dict[str, int] = statuses

        return casted

    def get_compilation_queue(self, db_name: str | None = None) -> list[Any]:
        """Очередь компиляции (пустой список - компиляция не идёт).

        Контракт kb-ui ``GET /api-studio/siem/compile/status``.

        :param db_name: Имя БД (None - первая deployable)
        :return: Сырой список задач компиляции
        """
        db_name = self.__resolve_db_name(db_name)
        queue = self.__studio_api_request(
            db_name, self.__api_compile_status, "get_compilation_queue"
        )
        casted: list[Any] = queue

        return casted

    # ------------------------------------------------------------------
    # Группа 3. Формы редактирования / клонирование / переименование правил
    # ------------------------------------------------------------------
    def get_content_rule_edit_form(
        self, db_name: str | None, content_type: str, rule_id: str
    ) -> dict[str, Any]:
        """Форма редактирования правила (отличается от ``get_content_rule``).

        Контракт kb-ui ``GET /api-studio/siem/{type}-rules/{id}/edit``.
        Возвращает ``ConstructorData``/``DeploymentSets``/``LocalizationRules``/
        ``SuggestedCloneName`` - то, что нужно для ``clone_content_rule`` и
        ``save_content_rule_form``.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentTypes
        :param rule_id: KB ID правила
        :return: Сырая форма редактирования (PascalCase)
        """
        db_name = self.__resolve_db_name(db_name)
        rule_path = self.__content_rule_path(content_type)
        api_path = f"{self.__api_siem}/{rule_path}/{rule_id}/edit"
        form = self.__studio_api_request(
            db_name, api_path, "get_content_rule_edit_form"
        )
        casted: dict[str, Any] = form

        return casted

    def clone_content_rule(
        self,
        db_name: str | None,
        content_type: str,
        rule_id: str,
        body: dict[str, Any] | None = None,
    ) -> str:
        """Клонировать правило.

        Контракт kb-ui ``PUT /api-studio/siem/{type}-rules/{id}/clone`` - тело
        это форма редактирования (``get_content_rule_edit_form``) с нужным
        ``SystemName``. Ответ - ``Id`` нового правила (голая UUID-строка). Если
        ``body`` не передан, форма берётся с сервера, а имя - из
        ``SuggestedCloneName``.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentTypes
        :param rule_id: KB ID исходного правила
        :param body: Форма клонирования (None - взять edit-форму и
            ``SuggestedCloneName``)
        :return: Идентификатор созданного правила
        """
        db_name = self.__resolve_db_name(db_name)
        rule_path = self.__content_rule_path(content_type)

        if body is None:
            form = self.get_content_rule_edit_form(db_name, content_type, rule_id)
            body = {k: v for k, v in form.items() if k != "SuggestedCloneName"}
            body["SystemName"] = form.get("SuggestedCloneName") or (
                f"{form.get('SystemName')}_copy"
            )

        api_path = f"{self.__api_siem}/{rule_path}/{rule_id}/clone"
        new_id = self.__studio_api_request(
            db_name, api_path, "clone_content_rule", method="PUT", body=body
        )
        casted: str = new_id

        return casted

    def preview_rename_content_rule(
        self,
        db_name: str | None,
        content_type: str,
        rule_id: str,
        new_system_name: str,
        old_system_name: str | None = None,
        formula: str | None = None,
        localization_rules: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Предпросмотр переименования правила (переименование НЕ сохраняет).

        Контракт kb-ui ``POST /api-studio/siem/{type}-rules/{id}/rename`` телом
        ``{newSystemName, oldSystemName, formula, localizationRules}``. Возвращает
        пересобранную формулу/локализации для нового имени; реальное сохранение -
        через ``save_content_rule_form`` (``PUT .../{id}``).

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentTypes
        :param rule_id: KB ID правила
        :param new_system_name: Новое системное имя
        :param old_system_name: Старое системное имя
        :param formula: Текущая формула
        :param localization_rules: Текущие правила локализации
        :return: {"new_system_name", "old_system_name", "formula",
            "localization_rules"}
        """
        db_name = self.__resolve_db_name(db_name)
        rule_path = self.__content_rule_path(content_type)
        api_path = f"{self.__api_siem}/{rule_path}/{rule_id}/rename"
        result = self.__studio_api_request(
            db_name,
            api_path,
            "preview_rename_content_rule",
            method="POST",
            body={
                "newSystemName": new_system_name,
                "oldSystemName": old_system_name,
                "formula": formula,
                "localizationRules": localization_rules or [],
            },
        )
        casted: dict[str, Any] = result

        return {
            "new_system_name": casted.get("NewSystemName"),
            "old_system_name": casted.get("OldSystemName"),
            "formula": casted.get("Formula"),
            "localization_rules": casted.get("LocalizationRules"),
        }

    def save_content_rule_form(
        self,
        db_name: str | None,
        content_type: str,
        rule_id: str,
        body: dict[str, Any],
    ) -> str:
        """Сохранить форму редактирования правила (реальное изменение).

        Контракт kb-ui ``PUT /api-studio/siem/{type}-rules/{id}`` (изменение
        имени/формулы/локализаций). Тело - форма из
        ``get_content_rule_edit_form`` с нужными правками.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentTypes
        :param rule_id: KB ID правила
        :param body: Форма редактирования (PascalCase)
        :return: Текст ответа (пустой при 200/204)
        """
        db_name = self.__resolve_db_name(db_name)
        rule_path = self.__content_rule_path(content_type)
        api_path = f"{self.__api_siem}/{rule_path}/{rule_id}"

        return cast(
            str,
            self.__studio_api_request(
                db_name,
                api_path,
                "save_content_rule_form",
                method="PUT",
                body=body,
            ),
        )

    def rename_content_rule(
        self,
        db_name: str | None,
        content_type: str,
        rule_id: str,
        new_system_name: str,
    ) -> None:
        """Переименовать правило (edit-form + preview rename + save).

        Комбинирует ``get_content_rule_edit_form`` ->
        ``preview_rename_content_rule`` -> ``save_content_rule_form``:
        системное имя и идентификаторы в формуле (``$id``, имя правила)
        обновляются согласованно, как это делает kb-ui.

        :param db_name: Имя БД (None - первая deployable)
        :param content_type: Тип объекта MPContentTypes
        :param rule_id: KB ID правила
        :param new_system_name: Новое системное имя
        """
        db_name = self.__resolve_db_name(db_name)
        form = self.get_content_rule_edit_form(db_name, content_type, rule_id)
        old_name = form.get("SystemName")
        preview = self.preview_rename_content_rule(
            db_name,
            content_type,
            rule_id,
            new_system_name=new_system_name,
            old_system_name=old_name,
            formula=form.get("Formula"),
            localization_rules=form.get("LocalizationRules"),
        )

        form["SystemName"] = new_system_name
        if preview.get("formula"):
            form["Formula"] = preview["formula"]
        form.pop("SuggestedCloneName", None)

        self.save_content_rule_form(db_name, content_type, rule_id, form)
        self.log.info(
            f'status=success, action=rename_content_rule, msg="renamed '
            f'{content_type} rule {old_name} -> {new_system_name}", '
            f'hostname="{self.__kb_hostname}", db="{db_name}"'
        )

    # ------------------------------------------------------------------
    # Группа 4. Управление базами контента (kb-ui databases API)
    # ------------------------------------------------------------------
    def get_content_databases_details(
        self, db_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Базы контента с деталями состояния (богаче ``get_databases_list``).

        Контракт kb-ui ``GET /api-studio/databases/content-databases``.

        :param db_name: Имя БД для заголовка (None - первая deployable)
        :return: [{"uid", "name", "display_name", "parent_name", "status",
            "is_updatable", "is_deployable", "is_updatable_to_restore",
            "revisions_count", "actions"}]
        """
        db_name = self.__resolve_db_name(db_name)
        databases = self.__studio_api_request(
            db_name, self.__api_content_databases, "get_content_databases_details"
        )

        return [
            {
                "uid": d.get("Uid"),
                "name": d.get("Name"),
                "display_name": d.get("DisplayName"),
                "parent_name": d.get("ParentName"),
                "status": d.get("Status"),
                "is_updatable": d.get("IsUpdatable"),
                "is_deployable": d.get("IsDeployable"),
                "is_updatable_to_restore": d.get("IsUpdatableToRestore"),
                "revisions_count": d.get("RevisionsCount"),
                "actions": d.get("Actions"),
            }
            for d in databases
        ]

    def get_content_database_tree_details(
        self, db_name: str | None, content_database: str | None = None
    ) -> dict[str, Any]:
        """Детали дерева базы контента (ревизии ветвления/слияния/подтяжки).

        Контракт kb-ui ``GET /api-studio/databases/content-databases/current/
        tree-details?contentDatabase=...``.

        :param db_name: Имя БД для заголовка (None - первая deployable)
        :param content_database: Имя БД в query (None - тот же db_name)
        :return: Сырой DTO tree-details (PascalCase)
        """
        db_name = self.__resolve_db_name(db_name)
        params = {"contentDatabase": content_database or db_name}
        details = self.__studio_api_request(
            db_name,
            self.__api_content_databases_tree_details,
            "get_content_database_tree_details",
            params=params,
        )
        casted: dict[str, Any] = details

        return casted

    def get_content_database_merge_details(self, db_name: str | None) -> dict[str, Any]:
        """Детали слияния базы контента (доступные действия ``Actions``).

        Контракт kb-ui ``GET /api-studio/databases/content-databases/current/
        merge-details``.

        :param db_name: Имя БД (None - первая deployable)
        :return: Сырой DTO merge-details (PascalCase)
        """
        db_name = self.__resolve_db_name(db_name)
        details = self.__studio_api_request(
            db_name,
            self.__api_content_databases_merge_details,
            "get_content_database_merge_details",
        )
        casted: dict[str, Any] = details

        return casted

    def get_content_database_tree_actions(self, db_name: str | None) -> dict[str, Any]:
        """Доступные действия над деревом базы контента.

        Контракт kb-ui ``GET /api-studio/databases/content-databases/current/
        tree-actions``.

        :param db_name: Имя БД (None - первая deployable)
        :return: {"CanViewDatabase": bool, ...}
        """
        db_name = self.__resolve_db_name(db_name)
        actions = self.__studio_api_request(
            db_name,
            self.__api_content_databases_tree_actions,
            "get_content_database_tree_actions",
        )
        casted: dict[str, Any] = actions

        return casted

    def get_content_database_parent_top_revision(self, db_name: str | None) -> int:
        """Верхняя ревизия родительской базы контента.

        Контракт kb-ui ``GET /api-studio/databases/content-databases/parent/
        revisions/top`` (ответ - голое число).

        :param db_name: Имя БД (None - первая deployable)
        :return: Номер ревизии
        """
        db_name = self.__resolve_db_name(db_name)
        revision = self.__studio_api_request(
            db_name,
            self.__api_content_databases_parent_top_revision,
            "get_content_database_parent_top_revision",
        )
        casted: int = revision

        return casted

    def get_content_database_revisions(
        self, db_name: str | None, min_revision: int = 1, take: int = 50
    ) -> list[dict[str, Any]]:
        """Ревизии базы контента.

        Контракт kb-ui ``GET /api-studio/databases/revisions?minRevision=&take=``.

        :param db_name: Имя БД (None - первая deployable)
        :param min_revision: Нижняя ревизия
        :param take: Размер страницы
        :return: [{"revision", "change_time", "user_description",
            "package_version"}]
        """
        db_name = self.__resolve_db_name(db_name)
        params = {"minRevision": min_revision, "take": take}
        revisions = self.__studio_api_request(
            db_name,
            self.__api_databases_revisions,
            "get_content_database_revisions",
            params=params,
        )

        return [
            {
                "revision": r.get("Revision"),
                "change_time": r.get("ChangeTime"),
                "user_description": r.get("UserDescription"),
                "package_version": r.get("PackageVersion"),
            }
            for r in revisions
        ]

    def get_revisions_diff(
        self,
        db_name: str | None,
        first_revision: int,
        second_revision: int,
        take: int = 10_000,
    ) -> dict[str, list[dict[str, Any]]]:
        """Разница двух ревизий базы контента.

        Контракт kb-ui (приватный, вне ``api_contracts_documentation.md``):
        ``GET /api-studio/databases/revisions/v2/{late}/diff/{early}/statistics``,
        затем по группе ``.../diff/{early}/{group}`` (список ресурсов) и по
        ресурсу ``.../diff/{early}/{group}/{id}`` (свойства с диффом).
        Порядок ревизий не важен: меньшая берётся как ``early``.

        Ресурсы, свойства которых менялись и вернулись к исходному значению
        в рамках диапазона (в web-интерфейсе - "изменено" с пустым списком),
        в результат не попадают.

        Внимание: выполняется по одному запросу на группу и на каждый
        изменённый ресурс - на больших ревизиях операция долгая.

        :param db_name: Имя БД (None - первая deployable)
        :param first_revision: Первая ревизия
        :param second_revision: Вторая ревизия
        :param take: Лимит изменений на группу
        :return: {group: [{resource_name: [{"name", "action",
            "old_value", "new_value"}]}]}
        """
        db_name = self.__resolve_db_name(db_name)
        early = min(int(first_revision), int(second_revision))
        late = max(int(first_revision), int(second_revision))
        diff_base = f"{self.__api_databases_revisions}/v2/{late}/diff/{early}"

        groups = self.__studio_api_request(
            db_name, f"{diff_base}/statistics", "get_revisions_diff"
        )

        revisions_diff: dict[str, list[dict[str, Any]]] = {}
        if isinstance(groups, list):
            for group in groups:
                identifier = group.get("Identifier")
                if not identifier:
                    continue
                group_diff = self.__revision_group_diff(
                    db_name, diff_base, str(identifier), take
                )
                if group_diff:
                    revisions_diff[str(identifier)] = group_diff

        return revisions_diff

    def __revision_group_diff(
        self, db_name: str, diff_base: str, group: str, take: int
    ) -> list[dict[str, Any]]:
        """Список изменённых ресурсов одной группы ревизий-диффа."""
        resources = self.__studio_api_request(
            db_name,
            f"{diff_base}/{group}",
            "get_revisions_diff",
            params={"take": take},
        )
        if not isinstance(resources, list):
            return []

        group_diff: list[dict[str, Any]] = []
        for resource in resources:
            resource_id = resource.get("Id")
            resource_name = resource.get("Name")
            if resource_id is None:
                continue
            detail = self.__studio_api_request(
                db_name,
                f"{diff_base}/{group}/{resource_id}",
                "get_revisions_diff",
            )
            properties = detail.get("Properties") if isinstance(detail, dict) else None
            changes = self.__scan_revision_changes(properties or [])
            if changes:
                group_diff.append({resource_name: changes})
        return group_diff

    @classmethod
    def __scan_revision_changes(
        cls, property_list: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Рекурсивно собрать изменения свойств и подсвойств ресурса."""
        changes: list[dict[str, Any]] = []
        for prop in property_list:
            if prop.get("Action") == "Nothing":
                continue
            diff_info = prop.get("DiffInfo") or {}
            old_value = diff_info.get("OldValue")
            new_value = diff_info.get("NewValue")
            if old_value != new_value:
                changes.append(
                    {
                        "name": (prop.get("Name") or {}).get("Name"),
                        "action": prop.get("Action"),
                        "old_value": old_value,
                        "new_value": new_value,
                    }
                )
            changes.extend(cls.__scan_revision_changes(prop.get("Children") or []))
        return changes

    def set_content_database_updatable(
        self, db_name: str | None, is_updatable: bool
    ) -> dict[str, Any]:
        """Разрешить/запретить обновление базы контента.

        Контракт kb-ui ``PUT /api-studio/databases/content-databases/current/
        is-updatable`` телом ``{"IsUpdatable": ...}``. На базе, у которой
        родитель не обновляемый, сервер отвечает 409
        ``Update.ContentDatabaseIsNotUpdatable``.

        :param db_name: Имя БД (None - первая deployable)
        :param is_updatable: Признак обновляемости
        :return: Текст/JSON ответа
        """
        db_name = self.__resolve_db_name(db_name)
        return cast(
            dict,
            self.__studio_api_request(
                db_name,
                self.__api_content_databases_is_updatable,
                "set_content_database_updatable",
                method="PUT",
                body={"IsUpdatable": is_updatable},
            ),
        )

    def set_content_database_deployable(self, db_name: str | None) -> str:
        """Сделать базу контента deployable.

        Контракт kb-ui ``PUT /api-studio/databases/content-databases/current/
        is-deployable`` (без тела).

        :param db_name: Имя БД (None - первая deployable)
        :return: Текст ответа
        """
        db_name = self.__resolve_db_name(db_name)
        return cast(
            str,
            self.__studio_api_request(
                db_name,
                self.__api_content_databases_is_deployable,
                "set_content_database_deployable",
                method="PUT",
            ),
        )

    def start_content_database_pull(
        self, db_name: str | None, till_revision: int | None = None
    ) -> str:
        """Запустить подтяжку изменений из родительской базы контента.

        Контракт kb-ui ``POST /api-studio/databases/content-databases/current/
        pull/start`` (тело - номер ревизии).

        :param db_name: Имя БД (None - первая deployable)
        :param till_revision: Ревизия, до которой подтянуть (None - верхняя)
        :return: Текст ответа
        """
        db_name = self.__resolve_db_name(db_name)
        return cast(
            str,
            self.__studio_api_request(
                db_name,
                self.__api_content_databases_pull_start,
                "start_content_database_pull",
                method="POST",
                body=till_revision,
            ),
        )

    def stop_content_database_pull(self, db_name: str | None) -> str:
        """Остановить подтяжку изменений из родительской базы контента.

        Контракт kb-ui ``POST /api-studio/databases/content-databases/current/
        pull/stop``. Если активной операции нет - 404 ``Merge.NotFound``.

        :param db_name: Имя БД (None - первая deployable)
        :return: Текст ответа
        """
        db_name = self.__resolve_db_name(db_name)
        return cast(
            str,
            self.__studio_api_request(
                db_name,
                self.__api_content_databases_pull_stop,
                "stop_content_database_pull",
                method="POST",
            ),
        )

    # ------------------------------------------------------------------
    # Группа 5. Массовые операции над объектами
    # ------------------------------------------------------------------
    def check_mass_delete(
        self,
        db_name: str | None,
        object_ids: list[str],
        src_folder_id: str | None = None,
        recursive: bool = False,
    ) -> dict[str, int]:
        """Предпросмотр массового удаления объектов.

        Контракт kb-ui ``POST /api-studio/siem/mass-operations/check-delete``.
        Сервер 26.0 пересекает ``selectedObjects.ids`` с ``filter.folderId`` -
        чтобы ``success`` был ненулевым, ``src_folder_id`` должна покрывать
        объекты (иначе выборка пуста).

        :param db_name: Имя БД (None - первая deployable)
        :param object_ids: GUID объектов (в одной папке)
        :param src_folder_id: Папка, в которой лежат объекты
        :param recursive: Искать объекты рекурсивно в поддереве ``src_folder_id``
        :return: {"success", "user", "user_installed"}
        """
        db_name = self.__resolve_db_name(db_name)
        result = self.__studio_api_request(
            db_name,
            self.__api_mass_operations_check_delete,
            "check_mass_delete",
            method="POST",
            body=self.__mass_selection_body(object_ids, src_folder_id, recursive),
        )
        casted: dict[str, int] = result

        return {
            "success": casted.get("Success", 0),
            "user": casted.get("User", 0),
            "user_installed": casted.get("UserInstalled", 0),
        }

    def check_mass_move(
        self,
        db_name: str | None,
        dst_folder_id: str,
        object_ids: list[str],
        src_folder_id: str | None = None,
        recursive: bool = False,
    ) -> dict[str, int]:
        """Предпросмотр массового перемещения объектов в папку.

        Контракт kb-ui ``POST /api-studio/siem/mass-operations/check-move``
        (``folderId`` - папка назначения; ``mode``/``selectedObjects`` на
        верхнем уровне + покрывающий ``filter``).

        :param db_name: Имя БД (None - первая deployable)
        :param dst_folder_id: Идентификатор папки назначения
        :param object_ids: GUID объектов (в одной папке)
        :param src_folder_id: Папка, в которой лежат объекты
        :param recursive: Искать объекты рекурсивно в поддереве ``src_folder_id``
        :return: {"success", "user", "user_installed"}
        """
        db_name = self.__resolve_db_name(db_name)
        body = self.__mass_selection_body(object_ids, src_folder_id, recursive)
        body["folderId"] = dst_folder_id
        result = self.__studio_api_request(
            db_name,
            self.__api_mass_operations_check_move,
            "check_mass_move",
            method="POST",
            body=body,
        )
        casted: dict[str, int] = result

        return {
            "success": casted.get("Success", 0),
            "user": casted.get("User", 0),
            "user_installed": casted.get("UserInstalled", 0),
        }

    def move_objects(
        self,
        db_name: str | None,
        dst_folder_id: str,
        object_ids: list[str],
        src_folder_id: str | None = None,
        recursive: bool = False,
    ) -> str:
        """Массово переместить объекты в папку.

        Контракт kb-ui ``PUT /api-studio/siem/mass-operations/move`` телом
        ``{folderId, filter: {mode, selectedObjects, filter}}`` (вложенная
        форма). Сервер 26.0 пересекает ``selectedObjects.ids`` с
        ``filter.folderId``: без покрывающего ``src_folder_id`` перемещение -
        silent no-op (``200`` без эффекта).

        :param db_name: Имя БД (None - первая deployable)
        :param dst_folder_id: Идентификатор папки назначения
        :param object_ids: GUID объектов (в одной папке)
        :param src_folder_id: Папка, в которой лежат объекты
        :param recursive: Искать объекты рекурсивно в поддереве ``src_folder_id``
        :return: Текст ответа (пустой при 200)
        """
        db_name = self.__resolve_db_name(db_name)
        body = {
            "folderId": dst_folder_id,
            "filter": self.__mass_selection_body(object_ids, src_folder_id, recursive),
        }

        return cast(
            str,
            self.__studio_api_request(
                db_name,
                self.__api_mass_operations_move,
                "move_objects",
                method="PUT",
                body=body,
            ),
        )

    def delete_objects(
        self,
        db_name: str | None,
        object_ids: list[str],
        src_folder_id: str | None = None,
        recursive: bool = False,
    ) -> str:
        """Массово удалить объекты.

        Контракт kb-ui ``PUT /api-studio/siem/mass-operations/delete`` телом
        ``{mode, selectedObjects, filter}``. Сервер 26.0 пересекает
        ``selectedObjects.ids`` с ``filter.folderId``: без покрывающего
        ``src_folder_id`` удаление - silent no-op (``200`` без эффекта).

        :param db_name: Имя БД (None - первая deployable)
        :param object_ids: GUID объектов (в одной папке)
        :param src_folder_id: Папка, в которой лежат объекты
        :param recursive: Искать объекты рекурсивно в поддереве ``src_folder_id``
        :return: Текст ответа (пустой при 200)
        """
        db_name = self.__resolve_db_name(db_name)
        return cast(
            str,
            self.__studio_api_request(
                db_name,
                self.__api_mass_operations_delete,
                "delete_objects",
                method="PUT",
                body=self.__mass_selection_body(object_ids, src_folder_id, recursive),
            ),
        )

    # ------------------------------------------------------------------
    # Группа 6. Прочее (настройки SIEM / поиск по короткому id / миграция)
    # ------------------------------------------------------------------
    def get_siem_settings_current(self, db_name: str | None = None) -> dict[str, Any]:
        """Текущие настройки SIEM (версии SDK и таксономии).

        Контракт kb-ui ``GET /api-studio/siem/siem-settings/current``.

        :param db_name: Имя БД (None - первая deployable)
        :return: {"system_sdk_version", "validation_sdk", "taxonomy_version"}
        """
        db_name = self.__resolve_db_name(db_name)
        settings_ = self.__studio_api_request(
            db_name, self.__api_siem_siem_settings_current, "get_siem_settings_current"
        )

        return {
            "system_sdk_version": settings_.get("SystemSdkVersion"),
            "validation_sdk": settings_.get("ValidationSdk"),
            "taxonomy_version": settings_.get("TaxonomyVersion"),
        }

    def get_object_by_short_id(
        self, db_name: str | None, short_id: str
    ) -> dict[str, Any]:
        """Найти объект по короткому ``ObjectId`` (``LOC-CR-1``).

        Контракт kb-ui ``GET /api-studio/siem/search/objectId/{shortId}``. Буква
        в ``Type`` раскрывается через ``SHORT_ID_TYPE_MAP``.

        :param db_name: Имя БД (None - первая deployable)
        :param short_id: Короткий идентификатор объекта
        :return: {"id", "name", "object_type"}
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_siem}/search/objectId/{short_id}"
        found = self.__studio_api_request(db_name, api_path, "get_object_by_short_id")
        if found is None:
            raise LookupError(f"Object not found by short id: {short_id}")

        return {
            "id": found.get("Id"),
            "name": found.get("Name"),
            "object_type": self.SHORT_ID_TYPE_MAP.get(found.get("Type", "")),
        }

    def get_migration_info(self, db_name: str | None = None) -> dict[str, Any]:
        """Признак и контекст миграции (kb-ui migration).

        Контракт kb-ui ``GET /api-studio/migration/migrationInfo``.

        :param db_name: Имя БД (None - первая deployable)
        :return: {"can_show", "deployment_set_name", "database_name"}
        """
        db_name = self.__resolve_db_name(db_name)
        info = self.__studio_api_request(
            db_name, self.__api_migration_info, "get_migration_info"
        )

        return {
            "can_show": info.get("CanShow"),
            "deployment_set_name": info.get("DeploymentSetName"),
            "database_name": info.get("DatabaseName"),
        }

    def get_content_distribution_applications(
        self, db_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Приложения распределения контента (пусто, если не настроено).

        Контракт kb-ui ``GET /api-studio/content-distribution/applications``.

        :param db_name: Имя БД (None - первая deployable)
        :return: Сырой список приложений
        """
        db_name = self.__resolve_db_name(db_name)
        apps = self.__studio_api_request(
            db_name,
            self.__api_content_distribution_applications,
            "get_content_distribution_applications",
        )
        casted: list[dict[str, Any]] = apps

        return casted

    def close(self) -> None:
        if self.__kb_session is not None:
            self.__kb_session.close()
        self.__conveyor.close()
