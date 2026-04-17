#!/usr/bin/env bash
# 定位 Gazebo / Ignition GUI 黑屏：先区分「仿真没起来」还是「只有 3D 视口不画」。
# 用法：bash tools/diagnose_gazebo_gui.sh
# （在 challenge_multi_drone 目录下，或传 CHALLENGE_DIR）
set -euo pipefail
CHALLENGE_DIR="${CHALLENGE_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
CFG="${CHALLENGE_DIR}/config_sim"
export GZ_SIM_RESOURCE_PATH="${GZ_SIM_RESOURCE_PATH:-}${GZ_SIM_RESOURCE_PATH:+:}${CFG}/gazebo/models:${CFG}/gazebo/worlds:${CFG}/gazebo/plugins:${CHALLENGE_DIR}/config_sim/world/models"
export IGN_GAZEBO_RESOURCE_PATH="${IGN_GAZEBO_RESOURCE_PATH:-}${IGN_GAZEBO_RESOURCE_PATH:+:}${GZ_SIM_RESOURCE_PATH}"

echo "========== 1) 显示 / 会话 =========="
echo "XDG_SESSION_TYPE=${XDG_SESSION_TYPE:-<unset>}"
echo "WAYLAND_DISPLAY=${WAYLAND_DISPLAY:-<unset>}"
echo "DISPLAY=${DISPLAY:-<unset>}"
echo

echo "========== 2) 显卡（lspci）=========="
if command -v lspci >/dev/null 2>&1; then
  lspci | grep -iE 'vga|3d|display' || true
else
  echo "lspci 不可用"
fi
echo

echo "========== 3) NVIDIA（若有）=========="
if command -v nvidia-smi >/dev/null 2>&1; then
  nvidia-smi -L 2>/dev/null || true
else
  echo "无 nvidia-smi（可能为纯核显或驱动未装 CLI）"
fi
echo

echo "========== 4) ign 命令 =========="
if command -v ign >/dev/null 2>&1; then
  ign version 2>/dev/null | head -5 || true
else
  echo "未找到 ign，请先 source ROS workspace（install/setup.bash）"
  exit 1
fi
echo

echo "========== 5) 仅服务器：能否加载 empty.sdf（约 4s）=========="
# 若此处失败 → 资源路径/世界文件/插件问题，不是「黑 GUI」。
if timeout 4s ruby "$(command -v ign)" gazebo empty.sdf -r -s -v 1 >/tmp/gz_headless_diag.log 2>&1; then
  echo "headless 正常退出（少见，可能极快加载完）"
else
  ec=$?
  if [[ "$ec" == "124" ]]; then
    echo "headless 在超时内保持运行 → 服务器侧加载正常（退出码 124 = timeout 预期）"
  else
    echo "headless 异常退出码=$ec，见 /tmp/gz_headless_diag.log"
  fi
fi
echo "--- 日志尾部 ---"
tail -20 /tmp/gz_headless_diag.log || true
echo

echo "========== 6) 结论（按上面结果自判）=========="
echo "• 若第 5 步超时且日志无 Error：仿真核心正常，黑屏几乎肯定是 GUI/OpenGL（双显卡、驱动、Wayland/Qt）。"
echo "• 请在 tmux 的 **platform → 第一个窗格**（跑 launch_simulation 的那格）向上翻，搜索："
echo "    GL context | EGL | ogre | Qt | RHI | Vulkan"
echo "• 若你在 Wayland 桌面：先试仅会话修复（不必改仓库）："
echo "    export QT_QPA_PLATFORM=xcb"
echo "• 若日志提示无 GL 上下文 / EGL：双显卡本可设 CW2_GAZEBO_USE_NVIDIA_PRIME=1 再 launch，或系统里对终端勾选「独显运行」。"
echo "• 若第 5 步就报错：把 /tmp/gz_headless_diag.log 发给维护者（那是世界/资源路径问题，不是显示器）。"
echo
echo "本脚本路径: $0"
