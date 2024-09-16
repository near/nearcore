import { createContext } from 'react';

export const ShowAsciiCharactersInHexContext = createContext<{
    showAscii: boolean;
    dispatch: React.Dispatch<boolean>;
}>({
    showAscii: false,
    dispatch: () => {},
});
