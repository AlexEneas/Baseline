# PyInstaller spec for Music Maintenance Suite GUI
# Build: pyinstaller MusicMaintenanceSuite.spec

from PyInstaller.utils.hooks import collect_submodules
from pathlib import Path

block_cipher = None
project_dir = Path(__file__).resolve().parent

hiddenimports = []
hiddenimports += collect_submodules('mutagen')

a = Analysis(
    ['app.py'],
    pathex=[str(project_dir)],
    binaries=[],
    datas=[
        (str(project_dir / 'MixedinKey'), 'MixedinKey'),
        (str(project_dir / 'Discogs'), 'Discogs'),
        (str(project_dir / 'README.md'), '.'),
    ],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MusicMaintenanceSuite',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MusicMaintenanceSuite',
)
