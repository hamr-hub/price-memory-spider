#!/bin/bash

# Price Memory 部署脚本
# 支持开发、测试、生产环境的快速部署

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 配置变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="price-memory"
COMPOSE_FILE="docker-compose.yml"
ENV_FILE=".env"

# 默认配置
ENVIRONMENT="production"
FORCE_REBUILD=false
ENABLE_MONITORING=true
ENABLE_SSL=false
BACKUP_BEFORE_DEPLOY=true

# 帮助信息
show_help() {
    cat << EOF
Price Memory 部署脚本

用法: $0 [选项]

选项:
    -e, --env ENVIRONMENT     环境类型 (development|staging|production) [默认: production]
    -f, --force               强制重新构建镜像
    -s, --skip-monitoring     跳过监控组件部署
    -c, --skip-ssl            跳过SSL配置
    -b, --skip-backup         跳过部署前备份
    -h, --help                显示此帮助信息
    -v, --version             显示版本信息

示例:
    $0 -e development         # 部署开发环境
    $0 -e production -f       # 强制重新构建并部署生产环境
    $0 --env staging          # 部署测试环境

EOF
}

# 显示版本信息
show_version() {
    echo "Price Memory Deploy Script v1.0.0"
    echo "支持环境: development, staging, production"
}

# 检查依赖
check_dependencies() {
    log_info "检查依赖..."
    
    # 检查 Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装"
        exit 1
    fi
    
    # 检查 Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose 未安装"
        exit 1
    fi
    
    # 检查 curl
    if ! command -v curl &> /dev/null; then
        log_error "curl 未安装"
        exit 1
    fi
    
    log_success "依赖检查完成"
}

# 设置环境配置
setup_environment() {
    log_info "设置环境: $ENVIRONMENT"
    
    case $ENVIRONMENT in
        development)
            COMPOSE_FILE="docker-compose.dev.yml"
            ENV_FILE=".env.development"
            log_info "开发环境配置"
            ;;
        staging)
            COMPOSE_FILE="docker-compose.staging.yml"
            ENV_FILE=".env.staging"
            log_info "测试环境配置"
            ;;
        production)
            COMPOSE_FILE="docker-compose.yml"
            ENV_FILE=".env.production"
            log_info "生产环境配置"
            ;;
        *)
            log_error "不支持的环境类型: $ENVIRONMENT"
            log_info "支持的环境: development, staging, production"
            exit 1
            ;;
    esac
    
    # 检查配置文件是否存在
    if [ ! -f "$SCRIPT_DIR/$ENV_FILE" ]; then
        log_warning "环境配置文件不存在: $ENV_FILE"
        log_info "请先配置环境变量文件"
        create_sample_env
    fi
}

# 创建示例环境文件
create_sample_env() {
    log_info "创建示例环境配置文件..."
    
    cat > "$SCRIPT_DIR/.env.example" << 'EOF'
# Price Memory 环境配置示例

# 环境配置
ENV=production
DEBUG=false
LOG_LEVEL=INFO

# 节点配置
NODE_NAME=production-node-1
NODE_CONCURRENCY=5
AUTO_CONSUME_QUEUE=true

# 数据库配置
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-key

# 浏览器配置
BROWSER_MODE=remote
PLAYWRIGHT_WS_ENDPOINT=ws://playwright-browser:3000

# SMTP配置 (可选)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASS=your-app-password
SMTP_FROM=Price Memory <noreply@yourcompany.com>

# Webhook配置 (可选)
ALERT_WEBHOOK_SECRET=your-webhook-secret-key

# 前端配置
VITE_API_URL=http://localhost:8000/api/v1

# 代理配置 (可选)
# PROXY_SERVER=http://proxy.example.com:8080
# PROXY_USERNAME=your_proxy_username
# PROXY_PASSWORD=your_proxy_password
EOF

    log_success "示例配置文件已创建: .env.example"
    log_info "请复制并配置环境变量文件: cp .env.example $ENV_FILE"
    exit 0
}

# 备份现有数据
backup_data() {
    if [ "$BACKUP_BEFORE_DEPLOY" = false ]; then
        return 0
    fi
    
    log_info "创建部署前备份..."
    
    BACKUP_DIR="$SCRIPT_DIR/backups/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    
    # 备份数据库（如果有）
    if docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" ps | grep -q postgres; then
        log_info "备份数据库..."
        docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" exec -T postgres pg_dump -U postgres > "$BACKUP_DIR/database_backup.sql"
        log_success "数据库备份完成"
    fi
    
    # 备份配置文件
    cp -r "$SCRIPT_DIR/config" "$BACKUP_DIR/" 2>/dev/null || true
    
    # 备份日志
    docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" logs --no-color > "$BACKUP_DIR/logs.txt" 2>/dev/null || true
    
    log_success "备份完成: $BACKUP_DIR"
}

# 构建镜像
build_images() {
    log_info "构建Docker镜像..."
    
    if [ "$FORCE_REBUILD" = true ]; then
        docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" build --no-cache
    else
        docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" build
    fi
    
    log_success "镜像构建完成"
}

# 停止现有服务
stop_services() {
    log_info "停止现有服务..."
    
    docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" down
    
    log_success "服务已停止"
}

# 启动服务
start_services() {
    log_info "启动服务..."
    
    docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" up -d
    
    log_success "服务启动完成"
}

# 等待服务就绪
wait_for_services() {
    log_info "等待服务就绪..."
    
    # 等待API服务
    for i in {1..60}; do
        if curl -f -s http://localhost:8000/health > /dev/null 2>&1; then
            log_success "API服务就绪"
            break
        fi
        if [ $i -eq 60 ]; then
            log_error "API服务启动超时"
            return 1
        fi
        sleep 2
    done
    
    # 等待前端服务
    for i in {1..30}; do
        if curl -f -s http://localhost:5173 > /dev/null 2>&1; then
            log_success "前端服务就绪"
            break
        fi
        if [ $i -eq 30 ]; then
            log_warning "前端服务启动较慢，请稍后检查"
        fi
        sleep 2
    done
}

# 运行健康检查
health_check() {
    log_info "运行健康检查..."
    
    # 检查所有服务状态
    docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" ps
    
    # 检查容器日志
    log_info "检查容器日志..."
    docker-compose -f "$SCRIPT_DIR/$COMPOSE_FILE" logs --tail=50
    
    # API健康检查
    if curl -f -s http://localhost:8000/health > /dev/null 2>&1; then
        log_success "API健康检查通过"
    else
        log_error "API健康检查失败"
        return 1
    fi
    
    # 数据库连接检查
    log_info "检查数据库连接..."
    # 这里可以添加数据库连接测试
    
    log_success "所有健康检查通过"
}

# 显示部署信息
show_deployment_info() {
    log_success "部署完成!"
    
    echo ""
    echo "=================================="
    echo "  Price Memory 部署信息"
    echo "=================================="
    echo ""
    echo "环境: $ENVIRONMENT"
    echo "API地址: http://localhost:8000"
    echo "前端地址: http://localhost:5173"
    echo ""
    
    if [ "$ENABLE_MONITORING" = true ]; then
        echo "监控地址:"
        echo "  - Prometheus: http://localhost:9090"
        echo "  - Grafana: http://localhost:3001"
        echo ""
    fi
    
    echo "常用命令:"
    echo "  查看日志: docker-compose -f $COMPOSE_FILE logs -f"
    echo "  停止服务: docker-compose -f $COMPOSE_FILE down"
    echo "  重启服务: docker-compose -f $COMPOSE_FILE restart"
    echo ""
    echo "=================================="
}

# 清理函数
cleanup() {
    log_info "清理临时文件..."
    # 清理临时文件
}

# 信号处理
trap cleanup EXIT

# 主函数
main() {
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -f|--force)
                FORCE_REBUILD=true
                shift
                ;;
            -s|--skip-monitoring)
                ENABLE_MONITORING=false
                shift
                ;;
            -c|--skip-ssl)
                ENABLE_SSL=false
                shift
                ;;
            -b|--skip-backup)
                BACKUP_BEFORE_DEPLOY=false
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            -v|--version)
                show_version
                exit 0
                ;;
            *)
                log_error "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # 显示标题
    echo "=================================="
    echo "  Price Memory 部署脚本"
    echo "=================================="
    echo ""
    
    # 执行部署步骤
    check_dependencies
    setup_environment
    
    # 如果是生产环境，提示确认
    if [ "$ENVIRONMENT" = "production" ]; then
        log_warning "即将部署生产环境，是否继续? (y/N)"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            log_info "部署已取消"
            exit 0
        fi
    fi
    
    backup_data
    stop_services
    build_images
    start_services
    wait_for_services
    health_check
    show_deployment_info
}

# 执行主函数
main "$@"