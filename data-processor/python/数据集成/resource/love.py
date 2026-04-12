import random
import threading
import time
from math import sin, cos, pi, log
from tkinter import *
# 用于播放音乐的库（需提前安装：pip install pygame）
import pygame

# -------------------------- 基础配置 --------------------------
# 画布配置（保持原爱心特效配置）
CANVAS_WIDTH = 640
CANVAS_HEIGHT = 640
CANVAS_CENTER_X = CANVAS_WIDTH / 2
CANVAS_CENTER_Y = CANVAS_HEIGHT / 2
IMAGE_ENLARGE = 11
HEART_COLOR = "#Fd798f"

# 音乐配置（请替换为你的本地音乐文件路径，支持MP3、WAV等格式）
LOCAL_MUSIC_PATH = "/M50000147z1E24VY6Z.mp3"


# -------------------------- 爱心特效核心函数（原逻辑保留） --------------------------
def heart_function(t, shrink_ratio: float = IMAGE_ENLARGE):
    x = 16 * (sin(t) ** 3)
    y = -(15 * cos(t) - 5 * cos(2 * t) - 2 * cos(3 * t) - cos(3 * t))
    x *= shrink_ratio
    y *= shrink_ratio
    x += CANVAS_CENTER_X
    y += CANVAS_CENTER_Y
    return int(x), int(y)


def scatter_inside(x, y, beta=0.15):
    ratio_x = -beta * log(random.random())
    ratio_y = -beta * log(random.random())
    dx = ratio_x * (x - CANVAS_CENTER_X)
    dy = ratio_y * (y - CANVAS_CENTER_Y)
    return x - dx, y - dy


def shrink(x, y, ratio):
    force = -1 / (((x - CANVAS_CENTER_X) ** 2 + (y - CANVAS_CENTER_Y) ** 2) ** 0.6)
    dx = ratio * force * (x - CANVAS_CENTER_X)
    dy = ratio * force * (y - CANVAS_CENTER_Y)
    return x - dx, y - dy


def curve(p):
    return 2 * (2 * sin(3 * p)) / (2 * pi)


class Heart:
    def __init__(self, generate_frame=20):
        self._points = set()
        self._edge_diffusion_points = set()
        self._center_diffusion_points = set()
        self.all_points = {}
        self.build(2000)
        self.random_halo = 1000
        self.generate_frame = generate_frame
        for frame in range(generate_frame):
            self.calc(frame)

    def build(self, number):
        for _ in range(number):
            t = random.uniform(0, 2 * pi)
            x, y = heart_function(t)
            self._points.add((x, y))
        for _x, _y in list(self._points):
            for _ in range(3):
                x, y = scatter_inside(_x, _y, 0.05)
                self._edge_diffusion_points.add((x, y))
        point_list = list(self._points)
        for _ in range(10000):
            x, y = random.choice(point_list)
            x, y = scatter_inside(x, y, 0.17)
            self._center_diffusion_points.add((x, y))

    @staticmethod
    def calc_position(x, y, ratio):
        force = 1 / (((x - CANVAS_CENTER_X) ** 2 + (y - CANVAS_CENTER_Y) ** 2) ** 0.520)
        dx = ratio * force * (x - CANVAS_CENTER_X) + random.randint(-1, 1)
        dy = ratio * force * (y - CANVAS_CENTER_Y) + random.randint(-1, 1)
        return x - dx, y - dy

    def calc(self, generate_frame):
        ratio = 25 * curve(generate_frame / 10 * pi)
        halo_radius = int(4 + 6 * (1 + curve(generate_frame / 10 * pi)))
        halo_number = int(3000 + 4000 * abs(curve(generate_frame / 10 * pi) ** 2))
        all_points = []
        heart_halo_point = set()
        for _ in range(halo_number):
            t = random.uniform(0, 2 * pi)
            x, y = heart_function(t, shrink_ratio=11.6)
            x, y = shrink(x, y, halo_radius)
            if (x, y) not in heart_halo_point:
                heart_halo_point.add((x, y))
                x += random.randint(-14, 14)
                y += random.randint(-14, 14)
                size = random.choice((1, 2, 2))
                all_points.append((x, y, size))
        for x, y in self._points:
            x, y = self.calc_position(x, y, ratio)
            size = random.randint(1, 3)
            all_points.append((x, y, size))
        for x, y in self._edge_diffusion_points:
            x, y = self.calc_position(x, y, ratio)
            size = random.randint(1, 2)
            all_points.append((x, y, size))
        for x, y in self._center_diffusion_points:
            x, y = self.calc_position(x, y, ratio)
            size = random.randint(1, 2)
            all_points.append((x, y, size))
        self.all_points[generate_frame] = all_points

    def render(self, render_canvas, render_frame):
        for x, y, size in self.all_points[render_frame % self.generate_frame]:
            render_canvas.create_rectangle(x, y, x + size, y + size, width=0, fill=HEART_COLOR)


def draw(main: Tk, render_canvas: Canvas, render_heart: Heart, render_frame=0):
    render_canvas.delete('all')
    render_heart.render(render_canvas, render_frame)
    main.after(160, draw, main, render_canvas, render_heart, render_frame + 1)


# -------------------------- 新增：音乐播放函数 --------------------------
def play_local_music(music_path):
    """单独线程播放本地音乐，避免阻塞爱心动画"""
    try:
        # 初始化pygame音乐模块
        pygame.mixer.init()
        # 加载音乐文件
        pygame.mixer.music.load(music_path)
        # 设置循环播放（-1表示无限循环，0表示播放1次）
        pygame.mixer.music.play(loops=-1)
        # 音乐播放时保持线程运行（直到窗口关闭）
        while pygame.mixer.music.get_busy():
            time.sleep(1)
    except Exception as e:
        print(f"音乐播放异常：{e}")
        print("请检查：1.音乐文件路径是否正确 2.是否安装pygame（pip install pygame）")


# -------------------------- 主函数（整合特效与音乐） --------------------------
if __name__ == '__main__':
    # 1. 启动音乐播放线程（单独线程，不影响UI动画）
    music_thread = threading.Thread(target=play_local_music, args=(LOCAL_MUSIC_PATH,), daemon=True)
    music_thread.start()

    # 2. 启动爱心特效（原逻辑保留）
    root = Tk()
    root.title("Animated Heart + Music")
    canvas = Canvas(root, bg='black', width=CANVAS_WIDTH, height=CANVAS_HEIGHT)
    canvas.pack()
    heart = Heart()
    draw(root, canvas, heart)

    # 3. 窗口关闭时停止音乐
    def stop_music():
        if pygame.mixer.get_init():
            pygame.mixer.music.stop()
            pygame.mixer.quit()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", stop_music)
    root.mainloop()