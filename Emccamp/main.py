import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

from src.pipeline import Pipeline
from src.config.loader import ConfigLoader
from src.utils.console_runner import run_with_console


BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"


if ENV_FILE.exists():
    load_dotenv(ENV_FILE)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python main.py", description="Pipeline EMCCAMP")

    # Argumento global para desabilitar espera
    parser.add_argument('--no-wait', action='store_true',
                       help='Nao aguarda antes de fechar o console')

    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("extract", help="Executa extracoes")
    extract.add_argument(
        "dataset",
        choices=["emccamp", "max", "judicial", "baixa", "doublecheck", "all"],
        help="Fonte de dados a extrair",
    )

    treat = subparsers.add_parser("treat", help="Executa tratamentos")
    treat.add_argument("dataset", choices=["emccamp", "max", "all"], help="Fonte de dados a tratar")

    subparsers.add_parser("batimento", help="Executa batimento EMCCAMP x MAX")
    subparsers.add_parser("baixa", help="Executa baixa MAX x EMCCAMP")
    subparsers.add_parser("devolucao", help="Executa devolucao MAX - EMCCAMP")
    enri = subparsers.add_parser("enriquecimento", help="Gera enriquecimento de contato")
    enri.add_argument(
        "--dataset",
        default="emccamp_batimento",
        help="Chave de configuracao em config.yaml para enriquecimento (padrao: emccamp_batimento)",
    )

    # Fluxo completo
    subparsers.add_parser("full", help="Executa fluxo completo (extract all + treat all + batimento + baixa + devolucao)")

    return parser


def handle_extract(args: argparse.Namespace, pipeline: Pipeline) -> None:
    if args.dataset in {"emccamp", "all"}:
        pipeline.extract_emccamp()

    if args.dataset in {"max", "all"}:
        pipeline.extract_max()

    if args.dataset in {"judicial", "all"}:
        pipeline.extract_judicial()

    if args.dataset in {"baixa", "all"}:
        pipeline.extract_baixa()

    if args.dataset in {"doublecheck", "all"}:
        pipeline.extract_doublecheck()


def handle_treat(args: argparse.Namespace, pipeline: Pipeline) -> None:
    if args.dataset in {"emccamp", "all"}:
        pipeline.treat_emccamp()
    if args.dataset in {"max", "all"}:
        pipeline.treat_max()


def _execute_pipeline(args: argparse.Namespace) -> None:
    """Executa o pipeline conforme os argumentos."""
    loader = ConfigLoader(base_path=BASE_DIR)
    pipeline = Pipeline(loader=loader)

    if args.command == "extract":
        handle_extract(args, pipeline)
    elif args.command == "treat":
        handle_treat(args, pipeline)
    elif args.command == "batimento":
        pipeline.batimento()
    elif args.command == "baixa":
        pipeline.baixa()
    elif args.command == "devolucao":
        pipeline.devolucao()
    elif args.command == "enriquecimento":
        pipeline.enriquecimento(args.dataset)
    elif args.command == "full":
        # Fluxo completo
        print("=" * 60)
        print("   PIPELINE COMPLETO EMCCAMP")
        print("=" * 60)

        print("\n[1/7] Extraindo EMCCAMP...")
        pipeline.extract_emccamp()

        print("\n[2/7] Extraindo MAX...")
        pipeline.extract_max()

        print("\n[3/7] Extraindo Judicial...")
        pipeline.extract_judicial()

        print("\n[4/7] Tratando EMCCAMP...")
        pipeline.treat_emccamp()

        print("\n[5/7] Tratando MAX...")
        pipeline.treat_max()

        print("\n[6/7] Executando Batimento...")
        pipeline.batimento()

        print("\n[7/7] Executando Baixa...")
        pipeline.baixa()

        print("\n" + "=" * 60)
        print("   PIPELINE COMPLETO FINALIZADO!")
        print("=" * 60)


def main(argv: list[str] | None = None) -> None:
    """Funcao principal com console runner."""
    parser = build_parser()
    args = parser.parse_args(argv)

    # Se --no-wait, executar diretamente sem console runner
    if args.no_wait:
        try:
            _execute_pipeline(args)
        except KeyboardInterrupt:
            print("\n Execucao interrompida pelo usuario")
            sys.exit(1)
        except Exception as e:
            print(f"\n Erro na execucao: {e}")
            sys.exit(1)
    else:
        # Executar com console runner (espera 5 minutos antes de fechar)
        exit_code = run_with_console(
            project_name="EMCCAMP",
            base_dir=BASE_DIR,
            pipeline_func=lambda: _execute_pipeline(args),
            wait_time=300,  # 5 minutos
        )
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
