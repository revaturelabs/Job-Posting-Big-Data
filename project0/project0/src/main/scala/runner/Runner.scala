package runner

import runner.bank.Cli

object Runner {
    def main(args: Array[String]) : Unit = {
        val game = new Cli()
        game.menu()
    }
}