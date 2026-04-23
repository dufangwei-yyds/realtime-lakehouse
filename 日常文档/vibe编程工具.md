# Vibe 编程工具指南

> AI 赋能开发，打造高效、智能的编程工作流

---

## 目录

- [Vibe 编程工具指南](#vibe-编程工具指南)
  - [目录](#目录)
  - [AI 代码编辑器](#ai-代码编辑器)
  - [零代码平台](#零代码平台)
  - [AI 应用开发平台](#ai-应用开发平台)
  - [AI 智能体平台](#ai-智能体平台)
  - [AI 模型](#ai-模型)
  - [AI 命令行编码工具](#ai-命令行编码工具)
  - [AI IDE 插件](#ai-ide-插件)
  - [AI 多媒体创作工具](#ai-多媒体创作工具)
  - [AI 浏览器插件](#ai-浏览器插件)
  - [AI 开发框架](#ai-开发框架)
  - [AI 大模型社区](#ai-大模型社区)
  - [AI Agent](#ai-agent)
  - [Agent Skills](#agent-skills)
    - [常用命令](#常用命令)
  - [Skills 安装管理工具](#skills-安装管理工具)
    - [使用方法](#使用方法)
    - [推荐配置](#推荐配置)
  - [Skills 资源平台](#skills-资源平台)
  - [Skills 开源合集](#skills-开源合集)
    - [官方与知名项目](#官方与知名项目)
    - [前端与设计](#前端与设计)
    - [数据库与后端](#数据库与后端)
    - [安全与运维](#安全与运维)
    - [自动化与效率](#自动化与效率)
    - [创意与媒体](#创意与媒体)
    - [开发方法论](#开发方法论)
    - [Obsidian](#obsidian)
  - [规范驱动开发](#规范驱动开发)
    - [Spec-kit 规范驱动开发](#spec-kit-规范驱动开发)
      - [安装](#安装)
      - [核心文件结构](#核心文件结构)
      - [开发流程 (7 步)](#开发流程-7-步)
      - [安装 CLI](#安装-cli)
    - [OpenSpec 轻量规范框架](#openspec-轻量规范框架)
      - [安装](#安装-1)
      - [目录结构](#目录结构)
      - [开发流程 (5 步)](#开发流程-5-步)
      - [详细操作](#详细操作)
      - [Spec-kit vs OpenSpec 对比](#spec-kit-vs-openspec-对比)
  - [aichat](#aichat)
    - [核心功能](#核心功能)
  - [MCP 服务](#mcp-服务)
  - [使用建议](#使用建议)

---

## AI 代码编辑器

| 工具              | 简介                       |
| ---------------- | ---------------------------|
| Cursor           | 主力狙击手（核心代码攻坚）      |
| Trae Solo        | 先遣部队（出原型、搞部署）      |
| WorkBuddy        | 金牌秘书（出方案）             |
| Qorder           | 外包团队（自主完成独立子模块开发）|
| Antigravity      | 预警机（全局扫描与审计）        |

---

## 零代码平台

> 快速做网站和应用

| 工具          | 简介                                    |
| -------------| ----------------------------------------|
| Bolt.new     | 原型设计和快速验证技术想法的最佳工具, 国外     |
| 秒悟          | 做内部办公系统、简单的SaaS原型或个人作品集,国内|
| 秒哒          | 做轻量应用、利用百度生态, 国内               |

---

## AI 应用开发平台

> 可视化配置 AI 应用

| 工具           | 简介               |
| -------------- | ------------------ |
| 阿里云百炼平台 | 可视化配置 AI 应用 |

---

## AI 智能体平台

> AI 自主规划和执行复杂任务，持续运行

| 工具      | 简介          |
| --------- | ------------- |
| Flowith   | AI 智能体平台 |
| Manus     | AI 智能体平台 |
| OpenManus | AI 智能体平台 |

---

## AI 模型

| 类别                 | 模型                                                       |
| -------------------- | ---------------------------------------------------------- |
| **国际主流**   | Claude、**ChatGPT**、**Gemini**                          |
| **国产大模型** | **DeepSeek-V3.2**、**智谱 GLM-5**、**Qwen3.5**          |
| **日常使用**   | **豆包** (日常)、**元宝** (微信)、MinMAX M2.5、Kimi-K2.5 |

---

## AI 命令行编码工具

| 工具        | 安装命令                                           |
| ----------- | -------------------------------------------------- |
| Claude Code | `curl -fsSL https://claude.ai/install.sh \| bash` |
| Gemini CLI  | `npm install -g @google/gemini-cli`              |
| OpenCode    | `curl -fsSL https://opencode.ai/install \| bash`  |
...

---

## AI IDE 插件

- **Cline**
- **CodeX**

---

## AI 多媒体创作工具

| 类别            | 工具        |
| --------------- | ----------- |
| AI 生图工具     | Nano Banana |
| AI 创作平台     | 即梦 AI     |
| AI 视频生成模型 | Veo3        |
| AI 语音合成工具 | Eleven Labs |

---

## AI 浏览器插件

AITDK、Monica、Web To MCP、百度文心助手

---

## AI 开发框架

| 框架              | 简介                                                |
| ----------------- | --------------------------------------------------- |
| Spring AI         | Java AI 开发的标配，提供统一 API 简化大模型集成     |
| LangChain4j       | Java 版的 LangChain，支持声明式语法，构建复杂 Agent |
| Vercel AI Gateway | AI 模型网关服务，让你能够统一调用数百个 AI 模型     |
| OpenRouter        | 统一的 AI 模型路由平台                              |

---

## AI 大模型社区

| 社区        | 简介          |
| ----------- | ------------- |
| HuggingFace | AI 大模型社区 |

---

## AI Agent

| Agent    | 简介     |
| -------- | -------- |
| QClaw  | AI Agent   |
| OpenClaw | AI Agent |

---

## Agent Skills

> Claude Code 技能系统

### 常用命令

```
/plugin list everything-claude-code@everything-claude-code
/plugin marketplace add anthropics/skills
/skills
skill install anthropic-agent-skills:frontend-design
```

---

## Skills 安装管理工具

| 工具          | 说明              | 链接                                            |
| ------------- | ----------------- | ----------------------------------------------- |
| skills CLI    | npm 包管理工具    | https://www.npmjs.com/package/skills            |
| find-skills   | 搜索 Skills       | `npx skills find`                             |
| skill-creator | 创建自定义 Skills | -                                               |
| Skill Seeker  | Skills 探索工具   | https://github.com/yusufkaraaslan/Skill_Seekers |

### 使用方法

```bash
# 添加 Skills
npx skills add <owner/repo>

# 添加官方 Skills
anthropics/skills | openai/skills
```

### 推荐配置

- **everything-claude-code**：Anthropic 黑客松冠军的完整配置集合
  - 包含：agents、skills、hooks、commands、rules、MCPs
  - 链接：https://github.com/affaan-m/everything-claude-code

---

## Skills 资源平台

| 平台                       | 链接                                 |
| -------------------------- | ------------------------------------ |
| Anthropic 官方技能仓库     | https://github.com/anthropics/skills |
| Claude Skills Hub          | https://www.claudeskill.site/        |
| Skills.sh                  | https://skills.sh                    |
| 鱼皮 AI 导航 - Skills 大全 | https://ai.codefather.cn/skills      |
| SkillsMP                   | https://skillsmp.com/zh              |
| MCPMarket                  | https://mcpmarket.com/daily/skills   |

---

## Skills 开源合集

### 官方与知名项目

| Skills                | 简介                            | 链接                                                                                       |
| --------------------- | ------------------------------- | ------------------------------------------------------------------------------------------ |
| anthropics/skills     | Anthropic 官方 Skills 仓库      | https://github.com/anthropics/skills                                                       |
| awesome-claude-skills | Skills 精选列表                 | -                                                                                          |
| openai/skills         | OpenAI 官方的 Codex Skills 目录 | -                                                                                          |
| Notion Skills         | Notion 官方 AI Skills           | https://www.notion.so/notiondevs/Notion-Skills-for-Claude-28da4445d27180c7af1df7d8615723d0 |

### 前端与设计

| Skills                      | 简介                                 | 链接                                                    |
| --------------------------- | ------------------------------------ | ------------------------------------------------------- |
| vercel-labs/agent-skills    | Vercel 出品的 React/Next.js 最佳实践 | -                                                       |
| expo/skills                 | Expo 官方的 React Native 开发 Skills | -                                                       |
| ui-ux-pro-max               | 专业前端设计 Skill                   | https://github.com/nextlevelbuilder/ui-ux-pro-max-skill |
| vercel-react-best-practices | Vercel 出品的 React 最佳实践         | -                                                       |
| web-design-guidelines       | Web 设计规范 Skill                   | -                                                       |
| frontend-design             | Anthropic 官方前端设计 Skill         | -                                                       |
| vue-skills                  | Vue.js 最佳实践 Skills               | https://github.com/vuejs-ai/skills                      |

### 数据库与后端

| Skills                           | 简介                                | 链接                                     |
| -------------------------------- | ----------------------------------- | ---------------------------------------- |
| supabase-postgres-best-practices | Supabase 出品的 PostgreSQL 最佳实践 | https://github.com/supabase/agent-skills |
| stripe/ai                        | Stripe 官方 AI Skills（支付处理）   | -                                        |

### 安全与运维

| Skills             | 简介                                             | 链接                                             |
| ------------------ | ------------------------------------------------ | ------------------------------------------------ |
| trailofbits/skills | Trail of Bits 安全公司出品（安全研究、漏洞检测） | https://github.com/trailofbits/skills            |
| seo-audit          | SEO 审计 Skill                                   | https://github.com/coreyhaines31/marketingskills |
| audit-website      | 网站安全审计 Skill                               | https://github.com/squirrelscan/skills           |

### 自动化与效率

| Skills        | 简介                               | 链接                                         |
| ------------- | ---------------------------------- | -------------------------------------------- |
| browser-use   | 让 AI Agent 能访问和操作网站的工具 | https://github.com/browser-use/browser-use   |
| agent-browser | Vercel 出品的浏览器自动化 Skill    | https://github.com/vercel-labs/agent-browser |

### 创意与媒体

| Skills              | 简介                                            | 链接                                   |
| ------------------- | ----------------------------------------------- | -------------------------------------- |
| remotion-dev/skills | Remotion 官方视频动画制作 Skills                | https://github.com/remotion-dev/skills |
| baoyu-skills        | 宝玉老师自用 Skills 集合（公众号、PPT、配图等） | https://github.com/JimLiu/baoyu-skills |
| heygen-com/skills   | HeyGen 官方 Skills（AI 数字人视频）             | https://github.com/heygen-com/skills   |
| humanizer           | 去除 AI 生成痕迹的 Skill                        | https://github.com/blader/humanizer    |

### 开发方法论

| Skills              | 简介                                   | 链接                                             |
| ------------------- | -------------------------------------- | ------------------------------------------------ |
| planning-with-files | 被 X 开发者评为最强 Skill！            | https://github.com/OthmanAdi/planning-with-files |
| superpowers         | 完整的 AI 编程技能框架和软件开发方法论 | https://github.com/obra/superpowers              |

### Obsidian

| Skills                 | 简介                        | 链接 |
| ---------------------- | --------------------------- | ---- |
| kepano/obsidian-skills | Obsidian 出品的 Skills 集合 | -    |

---

## 规范驱动开发

> 先把需求写成规范文档，并且把规范文档当作代码的唯一真相来源。AI 必须严格遵守这些条文来生成代码，确保产出完全符合预期。

### Spec-kit 规范驱动开发

**官网**：https://speckit.org/

#### 安装

```powershell
# 安装 uv
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# 配置环境变量
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")

# 初始化项目
uvx --from git+https://github.com/github/spec-kit.git specify init my-project
```

#### 核心文件结构

```
.specify/
├── memory/
│   └── constitution.md    # 项目的基本准则和约定
├── scripts/                # 可执行脚本
├── templates/              # 模板文件
.claude/
└── commands/              # 斜杠命令定义
```

#### 开发流程 (7 步)

| 步骤            | 命令                      | 说明                                               |
| --------------- | ------------------------- | -------------------------------------------------- |
| 1. Constitution | `/speckit.constitution` | 制定项目准则，定义基本原则、代码规范、性能标准     |
| 2. Specify      | `/speckit.specify`      | 编写功能规范，描述要做什么功能、为什么做、用户需求 |
| 3. Clarify      | `/speckit.clarify`      | 澄清不明确的地方（可选），AI 提出结构化问题        |
| 4. Plan         | `/speckit.plan`         | 制定技术方案，确定技术栈、系统架构、API 接口       |
| 5. Tasks        | `/speckit.tasks`        | 拆解任务，标注依赖关系和优先级                     |
| 6. Analyze      | `/speckit.analyze`      | 分析检查（可选），提前发现设计缺陷                 |
| 7. Implement    | `/speckit.implement`    | 执行实现，生成代码                                 |

#### 安装 CLI

```bash
uv tool install specify-cli --from git+https://github.com/github/spec-kit.git
```

---

### OpenSpec 轻量规范框架

#### 安装

```bash
npm install -g @fission-ai/openspec@latest
openspec init
```

#### 目录结构

```
openspec/
├── specs/                 # 主规范文档，记录项目完整现状
├── changes/               # 变更提案
├── AGENTS.md              # AI 编程助手操作指南
└── project.md             # 项目上下文说明
```

#### 开发流程 (5 步)

| 步骤               | 命令                   | 说明               |
| ------------------ | ---------------------- | ------------------ |
| 1. Draft           | `/openspec:proposal` | 起草变更提案       |
| 2. Verify & Review | `openspec validate`  | 验证和审查提案     |
| 3. Review          | 对话完善               | 和团队一起审查提案 |
| 4. Implement       | `/openspec:apply`    | 实现变更           |
| 5. Archive         | `/openspec:archive`  | 归档变更           |

#### 详细操作

```bash
# 创建变更提案
/openspec:proposal 添加功能：根据名称和邮箱搜索用户
# 或
openspec create-change add-user-search

# 查看变更
openspec list                                    # 查看所有变更
openspec validate add-user-search               # 验证格式
openspec show add-user-search                    # 查看详情

# 实现变更
/openspec:apply add-user-search

# 归档变更
openspec archive add-user-search --yes
```

#### Spec-kit vs OpenSpec 对比

| 特性     | Spec-kit                                         | OpenSpec                             |
| -------- | ------------------------------------------------ | ------------------------------------ |
| 流程     | 7 步（准则→需求→澄清→方案→任务→检查→代码） | 5 步（提案→审查→实现→归档→验证） |
| 适用场景 | 从 0 开始的大型新项目                            | 现有项目上迭代功能                   |
| 上手难度 | 较高                                             | 较低                                 |

---

## aichat

### 核心功能

- **Shell 助手**：给命令添加 `-e` 选项执行
- **智能补全**：通过快捷键智能补全命令
- **RAG 检索增强生成**：让 AI 基于本地文档回答问题
- **AI 代理**：把指令、工具和文档组合成自动化工作流
- **Web 界面**：运行 `aichat --serve` 启动本地网页界面，可同时对比多个模型的回答

---

## MCP 服务

| 服务                | 链接                                                  |
| ------------------- | ----------------------------------------------------- |
| Context7            | https://context7.com/                                 |
| Firecrawl           | https://www.firecrawl.dev/                            |
| GitHub MCP Server   | https://github.com/github/github-mcp-server           |
| Chrome DevTools MCP | https://github.com/ChromeDevTools/chrome-devtools-mcp |

---

## 使用建议

1. **入门推荐**：从 AI 代码编辑器 (Cursor/Trae) 开始，体验 AI 辅助编程
2. **效率提升**：安装 Claude Code 或 aichat，配合 Skills 使用
3. **规范开发**：大型项目使用 Spec-kit，小型迭代使用 OpenSpec
4. **持续学习**：关注 HuggingFace 和各厂商的模型更新

---

> 持续更新中，欢迎提交 Issue 补充更多工具和资源
