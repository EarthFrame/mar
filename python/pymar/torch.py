from typing import Any, Callable, List, Optional
from .core import MarArchive

class MarDataset:
    """
    PyTorch Dataset wrapper for random-access MAR archives.
    Fork-safe across DataLoader multi-processing workers (releases GIL during reads).
    """
    def __init__(self, archive_path: str, transform: Optional[Callable[[bytes], Any]] = None, filter_fn: Optional[Callable[[str], bool]] = None):
        self.archive_path = archive_path
        self.transform = transform
        self._archive = MarArchive(archive_path)
        all_files = self._archive.list_files()
        
        # Filter regular files
        self.filenames = []
        for f in all_files:
            info = self._archive.get_file_info(f)
            if info and info.type == "file":
                if filter_fn is None or filter_fn(f):
                    self.filenames.append(f)

    def __len__(self) -> int:
        return len(self.filenames)

    def __getitem__(self, idx: int) -> Any:
        fname = self.filenames[idx]
        data = self._archive.read_file(fname)
        if self.transform is not None:
            return self.transform(data)
        return fname, data
