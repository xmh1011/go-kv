#!/bin/bash
# 性能测试运行脚本
# 用于运行生产环境基准测试和端到端测试，并收集结果

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/test_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# 创建结果目录
mkdir -p "$RESULTS_DIR"

# 日志文件
LOG_FILE="$RESULTS_DIR/performance_test_${TIMESTAMP}.log"

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

# 运行基准测试
run_benchmarks() {
    print_header "运行生产环境基准测试 (gRPC + LSM)"

    cd "$PROJECT_ROOT"

    print_info "编译测试..."
    go test -c ./tests/ -o /tmp/perf_tests

    print_info "运行基准测试..."
    go test ./tests/... -bench=^BenchmarkProduction -benchtime=1s -run=^$ -benchmem \
        | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/benchmark_${TIMESTAMP}.txt"

    print_info "基准测试结果已保存到: $RESULTS_DIR/benchmark_${TIMESTAMP}.txt"
}

# 运行短时间端到端测试
run_short_e2e_tests() {
    print_header "运行短时间端到端测试 (3分钟)"

    cd "$PROJECT_ROOT"

    print_info "运行 3 分钟哈希校验测试..."
    go test ./tests/... -run=^TestLongRunning_HashVerification -timeout=10m \
        -v 2>&1 | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/e2e_hash_verify_${TIMESTAMP}.txt"

    print_info "运行 Follower 读取一致性测试..."
    go test ./tests/... -run=^TestLongRunning_FollowerReadConsistency -timeout=10m \
        -v 2>&1 | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/e2e_follower_${TIMESTAMP}.txt"

    print_success "短时间端到端测试完成"
}

# 运行长时间端到端测试
run_long_e2e_tests() {
    print_header "运行长时间端到端测试"

    cd "$PROJECT_ROOT"

    print_info "运行 5 分钟高并发写入测试..."
    go test ./tests/... -run=^TestLongRunning_5Min_WriteHeavy -timeout=15m \
        -v 2>&1 | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/e2e_5min_write_${TIMESTAMP}.txt"

    print_info "运行 5 分钟混合负载测试..."
    go test ./tests/... -run=^TestLongRunning_5Min_MixedWorkload -timeout=15m \
        -v 2>&1 | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/e2e_5min_mixed_${TIMESTAMP}.txt"

    print_success "长时间端到端测试完成"
}

# 运行所有端到端测试
run_all_e2e_tests() {
    print_header "运行所有端到端测试"

    cd "$PROJECT_ROOT"

    print_info "运行 5 分钟高并发写入测试..."
    timeout 900 go test ./tests/... -run=^TestLongRunning_5Min_WriteHeavy -v \
        2>&1 | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/e2e_5min_write_${TIMESTAMP}.txt" || true

    print_info "运行 5 分钟混合负载测试..."
    timeout 900 go test ./tests/... -run=^TestLongRunning_5Min_MixedWorkload -v \
        2>&1 | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/e2e_5min_mixed_${TIMESTAMP}.txt" || true

    print_info "运行 3 分钟哈希校验测试..."
    timeout 600 go test ./tests/... -run=^TestLongRunning_HashVerification -v \
        2>&1 | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/e2e_hash_verify_${TIMESTAMP}.txt" || true

    print_info "运行 Follower 读取一致性测试..."
    timeout 600 go test ./tests/... -run=^TestLongRunning_FollowerReadConsistency -v \
        2>&1 | tee -a "$LOG_FILE" \
        > "$RESULTS_DIR/e2e_follower_${TIMESTAMP}.txt" || true

    print_success "所有端到端测试完成"
}

# 解析测试结果并生成报告
parse_results() {
    print_header "解析测试结果"

    cd "$PROJECT_ROOT"

    # 创建汇总报告
    REPORT_FILE="$RESULTS_DIR/performance_report_${TIMESTAMP}.md"

    {
        echo "# 性能测试报告"
        echo ""
        echo "**测试时间**: $(date)"
        echo "**测试环境**: $(uname -a)"
        echo "**Go 版本**: $(go version)"
        echo ""

        echo "## 基准测试结果 (gRPC + LSM)"
        echo ""
        echo '```'
        cat "$RESULTS_DIR/benchmark_${TIMESTAMP}.txt" 2>/dev/null || echo "基准测试未运行"
        echo '```'
        echo ""

        echo "## 端到端测试结果"
        echo ""

        # 解析 5 分钟写入测试
        if [ -f "$RESULTS_DIR/e2e_5min_write_${TIMESTAMP}.txt" ]; then
            echo "### 5分钟高并发写入测试"
            echo ""
            echo "\`\`\`"
            grep -A 20 "TestLongRunning_5Min_WriteHeavy" "$RESULTS_DIR/e2e_5min_write_${TIMESTAMP}.txt" || true
            echo "\`\`\`"
            echo ""
        fi

        # 解析 5 分钟混合负载测试
        if [ -f "$RESULTS_DIR/e2e_5min_mixed_${TIMESTAMP}.txt" ]; then
            echo "### 5分钟混合负载测试"
            echo ""
            echo "\`\`\`"
            grep -A 20 "TestLongRunning_5Min_MixedWorkload" "$RESULTS_DIR/e2e_5min_mixed_${TIMESTAMP}.txt" || true
            echo "\`\`\`"
            echo ""
        fi

        # 解析哈希校验测试
        if [ -f "$RESULTS_DIR/e2e_hash_verify_${TIMESTAMP}.txt" ]; then
            echo "### 哈希校验测试"
            echo ""
            echo "\`\`\`"
            grep -A 20 "TestLongRunning_HashVerification" "$RESULTS_DIR/e2e_hash_verify_${TIMESTAMP}.txt" || true
            echo "\`\`\`"
            echo ""
        fi

        # 解析 Follower 读取测试
        if [ -f "$RESULTS_DIR/e2e_follower_${TIMESTAMP}.txt" ]; then
            echo "### Follower 读取一致性测试"
            echo ""
            echo "\`\`\`"
            grep -A 20 "TestLongRunning_FollowerReadConsistency" "$RESULTS_DIR/e2e_follower_${TIMESTAMP}.txt" || true
            echo "\`\`\`"
            echo ""
        fi

    } > "$REPORT_FILE"

    print_success "测试报告已生成: $REPORT_FILE"
}

# 显示使用说明
show_usage() {
    cat << EOF
性能测试运行脚本

用法:
    $0 [选项]

选项:
    --benchmarks      仅运行基准测试
    --short-e2e      仅运行短时间 E2E 测试
    --long-e2e       仅运行长时间 E2E 测试 (5分钟)
    --all-e2e        运行所有 E2E 测试
    --full           运行完整测试套件 (基准 + E2E)
    --parse-only     仅解析已有结果并生成报告
    --help          显示此帮助信息

示例:
    $0 --full                 # 运行完整测试套件
    $0 --benchmarks            # 仅运行基准测试
    $0 --all-e2e              # 运行所有 E2E 测试

结果目录: $RESULTS_DIR
日志文件: $LOG_FILE

EOF
}

# 主函数
main() {
    print_info "性能测试脚本启动"
    print_info "项目根目录: $PROJECT_ROOT"
    print_info "结果目录: $RESULTS_DIR"
    print_info "日志文件: $LOG_FILE"

    if [ $# -eq 0 ]; then
        show_usage
        exit 0
    fi

    case "$1" in
        --benchmarks)
            run_benchmarks
            parse_results
            ;;
        --short-e2e)
            run_short_e2e_tests
            parse_results
            ;;
        --long-e2e)
            run_long_e2e_tests
            parse_results
            ;;
        --all-e2e)
            run_all_e2e_tests
            parse_results
            ;;
        --full)
            run_benchmarks
            run_all_e2e_tests
            parse_results
            ;;
        --parse-only)
            parse_results
            ;;
        --help|-h)
            show_usage
            ;;
        *)
            print_error "未知选项: $1"
            show_usage
            exit 1
            ;;
    esac

    print_success "测试完成！"
    print_info "结果目录: $RESULTS_DIR"
}

# 运行主函数
main "$@"
