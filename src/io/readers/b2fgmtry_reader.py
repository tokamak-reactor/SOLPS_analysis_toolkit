import re
from typing import Tuple

class B2GeometryReader(BaseReader, FileBasedReaderMixin, B2fReadMixin):
    """
    Читатель для файлов геометрии B2.5 с поддержкой всех версий.
    Автоматически определяет версию и выбира appropriate парсер.
    """

    def __init__(self):
        super().__init__()
        self._version: Optional[str] = None
        self._is_legacy: Optional[bool] = None

    def read(self, file_path: Path) -> Dict[str, Any]:
        """
        Чтение файла геометрии с автоматическим определением версии.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Geometry file not found: {file_path}")

        with self.open(file_path):
            # Определяем версию из содержимого файла
            self._detect_version()

            # Выбираем соответствующий метод чтения
            if self._is_legacy:
                return self._read_legacy_format()
            else:
                return self._read_modern_format()

    def _detect_version(self) -> None:
        """
        Определяет версию формата по заголовку файла.
        """
        try:
            # Сохраняем позицию для восстановления
            current_pos = self._file_obj.tell()
            self._file_obj.seek(0)

            # Читаем первую строку для определения версии
            first_line = self._file_obj.readline().strip()

            # Восстанавливаем позицию
            self._file_obj.seek(current_pos)

            # Парсим версию с помощью регулярного выражения
            version_match = re.match(r'VERSION(\d{2})\.(\d{3})\.(\d{3})\s+\w+', first_line)

            if not version_match:
                # Если строка не соответствует ожидаемому формату
                raise ValueError(f"Invalid version format: {first_line}")

            # Извлекаем компоненты версии
            major = int(version_match.group(1))  # 03 → 3
            minor = int(version_match.group(2))  # 002 → 2
            patch = int(version_match.group(3))  # 000 → 0

            self._version = f"{major:02d}.{minor:03d}.{patch:03d}"

            # Определяем тип формата
            self._is_legacy = minor < 2  # Новый формат если minor >= 2

            # Восстанавливаем позицию чтения
            self._file_obj.seek(current_pos)

        except Exception as e:
            raise RuntimeError(f"Failed to detect file version: {e}")

    def _read_modern_format(self) -> Dict[str, Any]:
        """Чтение современного формата (>= 3.2.0)."""
        gmtry = {}

        # Пропускаем уже прочитанную строку с версией
        # Читаем остальные поля современного формата
        gmtry['nx'] = self._read_ifield('nx')
        gmtry['ny'] = self._read_ifield('ny')
        gmtry['rg'] = self._read_rfield('rg', dims=(gmtry['nx'],))
        # ... остальные поля

        gmtry['version'] = self._version
        gmtry['format_type'] = 'modern'

        return gmtry

    def _read_legacy_format(self) -> Dict[str, Any]:
        """Чтение и конвертация legacy формата (< 3.2.0)."""
        gmtry = {}

        # Чтение данных старого формата
        legacy_data = self._read_legacy_raw_data()

        # Конвертация в современный формат
        gmtry['nx'] = legacy_data['nx_old']
        gmtry['ny'] = legacy_data['ny_old']
        gmtry['rg'] = self._convert_legacy_rg(legacy_data)
        # ... конвертация остальных полей

        gmtry['version'] = self._version
        gmtry['format_type'] = 'legacy'
        gmtry['converted_from_legacy'] = True

        return gmtry

    def _read_legacy_raw_data(self) -> Dict[str, Any]:
        """Чтение сырых данных legacy формата."""
        data = {}
        # Специфичная логика чтения старого формата
        # ...
        return data

    def _convert_legacy_rg(self, legacy_data: Dict[str, Any]) -> np.ndarray:
        """Конвертация legacy rg в современный формат."""
        # Логика конвертации
        # ...
        return converted_data