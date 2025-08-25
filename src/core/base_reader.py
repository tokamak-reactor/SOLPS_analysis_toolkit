from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List
import numpy as np
import re
from pathlib import Path

class BaseReader(ABC):
    """Абстрактный базовый класс для всех читателей файлов."""

    # Словарь для автоматического подбора читателя по расширению файла
    # Ключ: расширение файла (в нижнем регистре, с точкой, например, '.dat')
    # Значение: класс, который будет обрабатывать этот файл
    _registry = {}

    @classmethod
    def register_reader(cls, extensions):
        """Декоратор для регистрации класса-читателя для указанных расширений."""
        def decorator(reader_class):
            for ext in extensions:
                ext = ext.lower()
                cls._registry[ext] = reader_class
            return reader_class
        return decorator

    @classmethod
    def get_reader(cls, file_path: Path):
        """Фабричный метод. Возвращает подходящий экземпляр читателя для файла."""
        ext = file_path.suffix.lower()
        reader_class = cls._registry.get(ext)
        if reader_class is None:
            raise ValueError(f"No reader registered for extension '{ext}'")
        return reader_class()

    @abstractmethod
    def read(self, file_path: Path) -> Dict[str, Any]:
        """
        Основной метод, который должен быть реализован в каждом конкретном читателе.
        Принимает путь к файлу.
        Возвращает словарь с данными (структура может быть своей для каждого типа файла).
        """
        pass

class B2fReadMixin:
    """
    Миксин для высокопроизводительного чтения текстовых данных в форматах B2.5.
    Оптимизирован для работы с большими файлами.
    Предполагается, что класс, который его использует, работает с файлом (имеет self._file_obj).
    """

    def _read_field(self, fieldname: str, dtype,
                    dims: Optional[Tuple[int, ...]] = None) -> np.ndarray:
        """
        Читает вещественное поле из текстового файла B2.5.
        Оптимизированная версия с использованием numpy.loadtxt для быстрого чтения.
        Работает со свойством класса self._file_obj

        Args:
            fieldname (str): Имя поля для поиска в файле
            dims (Tuple[int, ...], optional): Ожидаемая размерность массива
            dtype: Тип переменной Python, в котором будут сохранены данные из файла

        Returns:
            np.ndarray: Массив с прочитанными данными
        """

        if not hasattr(self, '_file_obj') or self._file_obj is None:
            raise RuntimeError("File object is not open. Call open() first.")

        current_pos = self._file_obj.tell()  # Запоминаем текущую позицию

        # Быстрый поиск заголовка с fieldname
        line = self._find_field_line(self._file_obj, fieldname)

        # Извлечение информации из заголовка
        parts = line.split()

        # Если dims не указана, извлекаем из заголовка
        if dims is None:
            try:
                num_elements = int(parts[2])
                dims = (num_elements,)
            except (IndexError, ValueError) as e:
                self._file_obj.seek(current_pos)  # Восстанавливаем позицию
                raise ValueError(f"Could not extract dimensions from line: {line.strip()}") from e

        # Проверка согласованности
        try:
            num_in_file = int(parts[2])
        except (IndexError, ValueError) as e:
            self._file_obj.seek(current_pos)
            raise ValueError(f"Could not parse number of elements from line: {line.strip()}") from e

        if num_in_file != np.prod(dims):
            self._file_obj.seek(current_pos)
            raise ValueError(f"Inconsistent number of elements. Expected {np.prod(dims)}, found {num_in_file}")

        try:
            # Читаем ровно нужное количество элементов
            field = np.loadtxt(self._file_obj, dtype=dtype, max_rows=((num_in_file + 15) // 16))

            # Проверяем, что прочитали достаточно данных
            if len(field) < num_in_file:
                # Добираем остальные данные если нужно
                remaining = num_in_file - len(field)
                additional_data = np.loadtxt(self._file_obj, dtype=dtype, max_rows=((remaining + 15) // 16))
                field = np.concatenate([field, additional_data])

            # Берем ровно нужное количество элементов
            field = field[:num_in_file]

        except Exception as e:
            self._file_obj.seek(current_pos)
            raise ValueError(f"Error reading data for field '{fieldname}': {e}") from e

         #Reshape если нужно
        if len(dims) > 1:
            field = field.reshape(dims)

        return field


    def _find_field_line(self, fieldname: str) -> str:
        """
        Быстрый поиск строки с указанным fieldname.
        Использует буферизованное чтение для больших файлов.
        """
        buffer_size = 8192  # Размер буфера для чтения
        buffer = ""
        pattern = re.compile(rf'.*{re.escape(fieldname)}.*')

        while True:
            chunk = self._file_obj.read(buffer_size)
            if not chunk:
                raise EOFError(f"EOF reached without finding '{fieldname}'")

            buffer += chunk
            lines = buffer.split('\n')

            # Проверяем все строки кроме последней (она может быть неполной)
            for line in lines[:-1]:
                if pattern.match(line):
                    # Возвращаем файловый указатель на начало следующей строки
                    lines_after_match = buffer.split(line)[1].split('\n')[1:]
                    reset_position = self._file_obj.tell() - len('\n'.join(lines_after_match).encode())
                    self._file_obj.seek(reset_position)
                    return line

            # Сохраняем последнюю неполную строку для следующей итерации
            buffer = lines[-1]

    def _read_rfield(self, fieldname: str, dims: Optional[Tuple[int, ...]] = None) -> np.ndarray:
        """
        Обертка для чтения вещественного поля (double precision, как в MATLAB).
        Соответствует MATLAB: double → np.float64
        """
        return self._read_field(fieldname, np.float64, dims)

    def _read_ifield(self, fieldname: str, dims: Optional[Tuple[int, ...]] = None) -> np.ndarray:
        """
        Обертка для чтения целочисленного поля со знаком (64-bit integer, как в MATLAB).
        Соответствует MATLAB: int64 → np.int64
        """
        return self._read_field(fieldname, np.int64, dims)

class FileBasedReaderMixin:
    """Миксин для читателей, работающих с файловыми объектами."""

    def __init__(self):
        self._file_obj = None
        self._file_path = None

    def open(self, file_path: Path) -> 'FileBasedReaderMixin':
        """Открывает файл для чтения."""
        if self._file_obj is not None:
            self.close()

        self._file_path = file_path
        self._file_obj = open(file_path, 'r')
        return self

    def close(self) -> None:
        """Закрывает файл."""
        if self._file_obj is not None:
            self._file_obj.close()
            self._file_obj = None
            self._file_path = None

    def __enter__(self) -> 'FileBasedReaderMixin':
        """Поддержка контекстного менеджера."""
        if self._file_obj is None and self._file_path is not None:
            self.open(self._file_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Гарантированное закрытие файла при выходе из контекста."""
        self.close()

    @property
    def is_open(self) -> bool:
        """Проверка, открыт ли файл."""
        return self._file_obj is not None
