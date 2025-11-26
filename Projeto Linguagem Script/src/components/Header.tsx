import { useContext, useState } from "react";
import { Button } from "./ui/button";
import { MinesweeperContext, MinesweeperDispatchContext } from "@/Context";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "./ui/select";
import SadEmoji from "./icons/SadEmoji";
import HappyEmoji from "./icons/HappyEmoji";

interface HeaderProps {
  tempo: number;
}

export default function Header({ tempo }: HeaderProps) {
  const dispatch = useContext(MinesweeperDispatchContext)!;
  const state = useContext(MinesweeperContext)!;
  const [i , setI] = useState(0);

  const updateTamTabuleiro = (val: string) => {
    const [linhas, colunas] = val.split("x").map(Number);
    dispatch({ type: "SET_TAM_TABULEIRO", payload: { linhas, colunas } });

    if (linhas === 9 && colunas === 9) {
      dispatch({ type: "SET_MINAS", payload: 10 });
    } else if (linhas === 16 && colunas === 16) {
      dispatch({ type: "SET_MINAS", payload: 40 });
    } else {
      dispatch({ type: "SET_MINAS", payload: 99 });
    }
  };

  const MostraBombas = () => {
    if (i === 0) {

      for (let i = 0; i < state.tamTabuleiro.linhas; i++) {
        for (let j = 0; j < state.tamTabuleiro.colunas; j++) {
          if (state.tabuleiro[i][j] === -999) {
             state.selecionados[i][j] = true;
          }
        }
      }

      setI(1);
    } else {
      
      for (let i = 0; i < state.tamTabuleiro.linhas; i++) {
        for (let j = 0; j < state.tamTabuleiro.colunas; j++) {
          if (state.tabuleiro[i][j] === -999) {
             state.selecionados[i][j] = false;
          }
        }
      }
      setI(0);
    }
  };
  return (
    <div
      className={
        "h-16 bg-[#003C43] w-full flex items-center justify-evenly p-2 text-white font-medium"
      }
    >
      <Select onValueChange={updateTamTabuleiro} defaultValue="9x9">
        <SelectTrigger className="w-[90px] dark">
          <SelectValue />
        </SelectTrigger>
        <SelectContent className="dark">
          <SelectItem value="9x9">9x9</SelectItem>
          <SelectItem value="16x16">16x16</SelectItem>
          <SelectItem value="16x30">16x30</SelectItem>
        </SelectContent>
      </Select>
      <Button
        title="Reiniciar o jogo"
        onClick={() => {
          dispatch({ type: "SET_RESET", payload: true });
        }}
      >
        {state.gameOver && !state.playerWon ? <SadEmoji /> : <HappyEmoji />}
      </Button>

      <Button className="w-[100px] "
        title="Reiniciar o jogo"
        onClick={MostraBombas}
      >
       MostrarBombas
      </Button>

      <div className="flex items-center">
        <div className="flex items-center w-12 mr-2">
          🚩 {state.minasRestantes}
        </div>
        <div className="flex items-center w-12 text-nowrap">⏱️ {tempo}</div>
      </div>
    </div>
  );
}
